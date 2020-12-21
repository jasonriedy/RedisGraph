/*
* Copyright 2018-2020 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "graph.h"
#include "RG.h"
#include "config.h"
#include "../util/arr.h"
#include "../util/qsort.h"
#include "../GraphBLASExt/GxB_Delete.h"
#include "../GraphBLASExt/GxB_Matrix_tuple_iter.h"
#include "../util/rmalloc.h"
#include "../util/datablock/oo_datablock.h"

// GraphBLAS Select operator to free edge arrays and delete edges.
static GxB_SelectOp _select_delete_edges = NULL;

/* ========================= Forward declarations  ========================= */
void _MatrixResizeToCapacity(const Graph *g, RG_Matrix m);

/* ========================= RG_Matrix functions =============================== */

// Creates a new matrix
static RG_Matrix RG_Matrix_New(GrB_Type data_type, GrB_Index nrows, GrB_Index ncols) {
	RG_Matrix matrix = rm_calloc(1, sizeof(_RG_Matrix));

	matrix->allow_multi_edge = true;

	GrB_Info matrix_res = GrB_Matrix_new(&matrix->grb_matrix, data_type, nrows, ncols);
	ASSERT(matrix_res == GrB_SUCCESS);

	int mutex_res = pthread_mutex_init(&matrix->mutex, NULL);
	ASSERT(mutex_res == 0);

	return matrix;
}

// Returns underlying GraphBLAS matrix.
static inline GrB_Matrix RG_Matrix_Get_GrB_Matrix(RG_Matrix matrix) {
	return matrix->grb_matrix;
}

// Locks the matrix.
static inline void RG_Matrix_Lock(RG_Matrix matrix) {
	pthread_mutex_lock(&matrix->mutex);
}

// Unlocks the matrix.
static inline void _RG_Matrix_Unlock(RG_Matrix matrix) {
	pthread_mutex_unlock(&matrix->mutex);
}

static inline bool _RG_Matrix_MultiEdgeEnabled(RG_Matrix matrix) {
	return matrix->allow_multi_edge;
}

// Free RG_Matrix.
static void RG_Matrix_Free(RG_Matrix matrix) {
	GrB_Matrix_free(&matrix->grb_matrix);
	pthread_mutex_destroy(&matrix->mutex);
	rm_free(matrix);
}

/* ========================= Synchronization functions ========================= */

/* Acquire a lock that does not restrict access from additional reader threads */
void Graph_AcquireReadLock(Graph *g) {
	pthread_rwlock_rdlock(&g->_rwlock);
}

/* Acquire a lock for exclusive access to this graph's data */
void Graph_AcquireWriteLock(Graph *g) {
	pthread_rwlock_wrlock(&g->_rwlock);
	g->_writelocked = true;
}

/* Release the held lock */
void Graph_ReleaseLock(Graph *g) {
	/* Set _writelocked to false BEFORE unlocking
	 * if this is a reader thread no harm done,
	 * if this is a writer thread the writer is about to unlock so once again
	 * no harm done, if we set `_writelocked` to false after unlocking it is possible
	 * for a reader thread to be considered as writer, performing illegal access to
	 * underline matrices, consider a context switch after unlocking `_rwlock` but
	 * before setting `_writelocked` to false. */
	g->_writelocked = false;
	pthread_rwlock_unlock(&g->_rwlock);
}

/* Writer request access to graph. */
void Graph_WriterEnter(Graph *g) {
	pthread_mutex_lock(&g->_writers_mutex);
}

/* Writer release access to graph. */
void Graph_WriterLeave(Graph *g) {
	pthread_mutex_unlock(&g->_writers_mutex);
}

/* Force execution of all pending operations on a matrix. */
static inline void _Graph_ApplyPending(GrB_Matrix m) {
	GrB_Index nvals;
	GrB_Info res = GrB_Matrix_nvals(&nvals, m);
	ASSERT(res == GrB_SUCCESS);
}

/* ========================= Graph utility functions ========================= */

// Return number of nodes graph can contain.
size_t _Graph_NodeCap(const Graph *g) {
	return g->nodes->itemCap;
}

// Return number of nodes graph can contain.
size_t _Graph_EdgeCap(const Graph *g) {
	return g->edges->itemCap;
}

// Locates edges connecting src to destination.
void _Graph_GetEdgesConnectingNodes(const Graph *g, NodeID src, NodeID dest, int r, Edge **edges) {
	ASSERT(g && src < Graph_RequiredMatrixDim(g) && dest < Graph_RequiredMatrixDim(g) &&
		   r < Graph_RelationTypeCount(g));

	Edge e;
	EdgeID edgeId;
	e.relationID = r;
	e.srcNodeID = src;
	e.destNodeID = dest;

	// relation map, maps (src, dest, r) to edge IDs.
	GrB_Matrix relation = Graph_GetRelationMatrix(g, r);
	GrB_Info res = GrB_Matrix_extractElement_UINT64(&edgeId, relation, src, dest);

	// No entry at [dest, src], src is not connected to dest with relation R.
	if(res == GrB_NO_VALUE) return;

	if(SINGLE_EDGE(edgeId)) {
		// Discard most significate bit.
		edgeId = SINGLE_EDGE_ID(edgeId);
		e.entity = DataBlock_GetItem(g->edges, edgeId);
		e.id = edgeId;
		ASSERT(e.entity);
		*edges = array_append(*edges, e);
	} else {
		/* Multiple edges connecting src to dest,
		 * entry is a pointer to an array of edge IDs. */
		EdgeID *edgeIds = (EdgeID *)edgeId;
		int edgeCount = array_len(edgeIds);

		for(int i = 0; i < edgeCount; i++) {
			edgeId = edgeIds[i];
			e.entity = DataBlock_GetItem(g->edges, edgeId);
			e.id = edgeId;
			ASSERT(e.entity);
			*edges = array_append(*edges, e);
		}
	}
}

// Tests if there's an edge of type r between src and dest nodes.
bool Graph_EdgeExists(const Graph *g, NodeID srcID, NodeID destID, int r) {
	ASSERT(g);
	EdgeID edgeId;
	GrB_Matrix M = Graph_GetRelationMatrix(g, r);
	GrB_Info res = GrB_Matrix_extractElement_UINT64(&edgeId, M, destID, srcID);
	return res == GrB_SUCCESS;
}

static inline Entity *_Graph_GetEntity(const DataBlock *entities, EntityID id) {
	return DataBlock_GetItem(entities, id);
}

/* ============= Matrix synchronization and resizing functions =============== */

/* Resize given matrix, such that its number of row and columns
 * matches the number of nodes in the graph. Also, synchronize
 * matrix to execute any pending operations. */
void _MatrixSynchronize(const Graph *g, RG_Matrix rg_matrix) {
	GrB_Matrix m = RG_Matrix_Get_GrB_Matrix(rg_matrix);
	GrB_Index n_rows;
	GrB_Index n_cols;
	GrB_Matrix_nrows(&n_rows, m);
	GrB_Matrix_ncols(&n_cols, m);
	GrB_Index dims = Graph_RequiredMatrixDim(g);

	// If the graph belongs to one thread, we don't need to lock the mutex.
	if(g->_writelocked) {
		if((n_rows != dims) || (n_cols != dims)) {
			GrB_Info res = GxB_Matrix_resize(m, dims, dims);
			ASSERT(res == GrB_SUCCESS);
		}

		// Writer under write lock, no need to flush pending changes.
		return;
	}
	// Lock the matrix.
	RG_Matrix_Lock(rg_matrix);

	// If the matrix has pending operations or requires
	// a resize, enter critical section.
	if((n_rows != dims) || (n_cols != dims)) {
		// Double-check if resize is necessary.
		GrB_Matrix_nrows(&n_rows, m);
		GrB_Matrix_ncols(&n_cols, m);
		dims = Graph_RequiredMatrixDim(g);
		if((n_rows != dims) || (n_cols != dims)) {
			GrB_Info res = GxB_Matrix_resize(m, dims, dims);
			ASSERT(res == GrB_SUCCESS);
		}
		// Flush changes to matrix.
		_Graph_ApplyPending(m);
	}
	// Unlock matrix mutex.
	_RG_Matrix_Unlock(rg_matrix);
}

/* Resize matrix to node capacity. */
void _MatrixResizeToCapacity(const Graph *g, RG_Matrix matrix) {
	GrB_Matrix m = RG_Matrix_Get_GrB_Matrix(matrix);
	GrB_Index nrows;
	GrB_Index ncols;
	GrB_Matrix_ncols(&ncols, m);
	GrB_Matrix_nrows(&nrows, m);
	GrB_Index cap = _Graph_NodeCap(g);

	// This policy should only be used in a thread-safe context, so no locking is required.
	if(ncols != cap || nrows != cap) {
		GrB_Info res = GxB_Matrix_resize(m, cap, cap);
		ASSERT(res == GrB_SUCCESS);
	}
}

/* Do not update matrices. */
void _MatrixNOP(const Graph *g, RG_Matrix matrix) {
	return;
}

/* Define the current behavior for matrix creations and retrievals on this graph. */
void Graph_SetMatrixPolicy(Graph *g, MATRIX_POLICY policy) {
	switch(policy) {
	case SYNC_AND_MINIMIZE_SPACE:
		// Default behavior; forces execution of pending GraphBLAS operations
		// when appropriate and sizes matrices to the current node count.
		g->SynchronizeMatrix = _MatrixSynchronize;
		break;
	case RESIZE_TO_CAPACITY:
		// Bulk insertion and creation behavior; does not force pending operations
		// and resizes matrices to the graph's current node capacity.
		g->SynchronizeMatrix = _MatrixResizeToCapacity;
		break;
	case DISABLED:
		// Used when deleting or freeing a graph; forces no matrix updates or resizes.
		g->SynchronizeMatrix = _MatrixNOP;
		break;
	default:
		ASSERT(false);
	}
}

/* Retrieve the current behavior for matrix creations and retrievals on this graph. */
MATRIX_POLICY Graph_GetMatrixPolicy(const Graph *g) {
     if (g->SynchronizeMatrix == _MatrixSynchronize) return SYNC_AND_MINIMIZE_SPACE;
     if (g->SynchronizeMatrix == _MatrixResizeToCapacity) return RESIZE_TO_CAPACITY;
     if (g->SynchronizeMatrix == _MatrixNOP) return DISABLED;
     ASSERT(false);
     return DISABLED;
}

/* Synchronize and resize all matrices in graph. */
void Graph_ApplyAllPending(Graph *g) {
	RG_Matrix M;

	for(int i = 0; i < array_len(g->labels); i ++) {
		M = g->labels[i];
		g->SynchronizeMatrix(g, M);
	}

	for(int i = 0; i < array_len(g->relations); i ++) {
		M = g->relations[i];
		g->SynchronizeMatrix(g, M);
	}

	bool maintain_transpose;
	Config_Option_get(Config_MAINTAIN_TRANSPOSE, &maintain_transpose);

	if(maintain_transpose) {
		for(int i = 0; i < array_len(g->t_relations); i ++) {
			M = g->t_relations[i];
			g->SynchronizeMatrix(g, M);
		}
	}
}

/* ================================ Graph API ================================ */
Graph *Graph_New(size_t node_cap, size_t edge_cap) {
	node_cap = MAX(node_cap, GRAPH_DEFAULT_NODE_CAP);
	edge_cap = MAX(node_cap, GRAPH_DEFAULT_EDGE_CAP);

	Graph *g = rm_malloc(sizeof(Graph));
	g->nodes = DataBlock_New(node_cap, sizeof(Entity), (fpDestructor)FreeEntity);
	g->edges = DataBlock_New(edge_cap, sizeof(Entity), (fpDestructor)FreeEntity);
	g->labels = array_new(RG_Matrix, GRAPH_DEFAULT_LABEL_CAP);
	g->relations = array_new(RG_Matrix, GRAPH_DEFAULT_RELATION_TYPE_CAP);
	g->adjacency_matrix = RG_Matrix_New(GrB_BOOL, node_cap, node_cap);
	g->_t_adjacency_matrix = RG_Matrix_New(GrB_BOOL, node_cap, node_cap);
	g->_zero_matrix = RG_Matrix_New(GrB_BOOL, node_cap, node_cap);

	// If we're maintaining transposed relation matrices, allocate a new array, otherwise NULL-set the pointer.
	bool maintain_transpose;
	Config_Option_get(Config_MAINTAIN_TRANSPOSE, &maintain_transpose);
	g->t_relations = maintain_transpose ?
					 array_new(RG_Matrix, GRAPH_DEFAULT_RELATION_TYPE_CAP) : NULL;

	// Initialize a read-write lock scoped to the individual graph
	int res;
	UNUSED(res);
	res = pthread_rwlock_init(&g->_rwlock, NULL);
	ASSERT(res == 0);
	g->_writelocked = false;

	// Force GraphBLAS updates and resize matrices to node count by default
	Graph_SetMatrixPolicy(g, SYNC_AND_MINIMIZE_SPACE);

	// Synchronization objects initialization.
	res = pthread_mutex_init(&g->_writers_mutex, NULL);
	ASSERT(res == 0);

	return g;
}

// All graph matrices are required to be squared NXN
// where N is Graph_RequiredMatrixDim.
size_t Graph_RequiredMatrixDim(const Graph *g) {
	// Matrix dimensions should be at least:
	// Number of nodes + number of deleted nodes.
	return g->nodes->itemCount + array_len(g->nodes->deletedIdx);
}

size_t Graph_NodeCount(const Graph *g) {
	ASSERT(g);
	return g->nodes->itemCount;
}

uint Graph_DeletedNodeCount(const Graph *g) {
	ASSERT(g);
	return DataBlock_DeletedItemsCount(g->nodes);
}

size_t Graph_LabeledNodeCount(const Graph *g, int label) {
	GrB_Index nvals = 0;
	GrB_Matrix m = Graph_GetLabelMatrix(g, label);
	if(m) GrB_Matrix_nvals(&nvals, m);
	return nvals;
}

size_t Graph_EdgeCount(const Graph *g) {
	ASSERT(g);
	return g->edges->itemCount;
}

uint Graph_DeletedEdgeCount(const Graph *g) {
	ASSERT(g);
	return DataBlock_DeletedItemsCount(g->edges);
}

int Graph_RelationTypeCount(const Graph *g) {
	return array_len(g->relations);
}

int Graph_LabelTypeCount(const Graph *g) {
	return array_len(g->labels);
}

void Graph_AllocateNodes(Graph *g, size_t n) {
	ASSERT(g);
	DataBlock_Accommodate(g->nodes, n);
}

void Graph_AllocateEdges(Graph *g, size_t n) {
	ASSERT(g);
	DataBlock_Accommodate(g->edges, n);
}

int Graph_GetNode(const Graph *g, NodeID id, Node *n) {
	ASSERT(g);
	n->entity = _Graph_GetEntity(g->nodes, id);
	n->id = id;
	return (n->entity != NULL);
}

int Graph_GetEdge(const Graph *g, EdgeID id, Edge *e) {
	ASSERT(g && id < _Graph_EdgeCap(g));
	e->entity = _Graph_GetEntity(g->edges, id);
	e->id = id;
	return (e->entity != NULL);
}

int Graph_GetNodeLabel(const Graph *g, NodeID nodeID) {
	ASSERT(g);
	int label = GRAPH_NO_LABEL;
	for(int i = 0; i < array_len(g->labels); i++) {
		bool x = false;
		GrB_Matrix M = Graph_GetLabelMatrix(g, i);
		GrB_Info res = GrB_Matrix_extractElement_BOOL(&x, M, nodeID, nodeID);
		if(res == GrB_SUCCESS && x == true) {
			label = i;
			break;
		}
	}

	return label;
}

int Graph_GetEdgeRelation(const Graph *g, Edge *e) {
	ASSERT(g && e);
	NodeID srcNodeID = Edge_GetSrcNodeID(e);
	NodeID destNodeID = Edge_GetDestNodeID(e);
	EdgeID id = ENTITY_GET_ID(e);

	// Search for relation mapping matrix M, where
	// M[dest,src] == edge ID.
	uint relationship_count = array_len(g->relations);
	for(uint i = 0; i < relationship_count; i++) {
		EdgeID edgeId = 0;
		GrB_Matrix M = Graph_GetRelationMatrix(g, i);
		GrB_Info res = GrB_Matrix_extractElement_UINT64(&edgeId, M, srcNodeID, destNodeID);
		if(res != GrB_SUCCESS) continue;

		if(SINGLE_EDGE(edgeId)) {
			EdgeID curEdgeID = SINGLE_EDGE_ID(edgeId);
			if(curEdgeID == id) {
				Edge_SetRelationID(e, i);
				return i;
			}
		} else {
			/* Multiple edges exists between src and dest
			 * see if given edge is one of them. */
			EdgeID *edges = (EdgeID *)edgeId;
			int edge_count = array_len(edges);
			for(int j = 0; j < edge_count; j++) {
				if(edges[j] == id) {
					Edge_SetRelationID(e, i);
					return i;
				}
			}
		}
	}

	// We must be able to find edge relation.
	ASSERT(false);
	return GRAPH_NO_RELATION;
}

void Graph_GetEdgesConnectingNodes(const Graph *g, NodeID srcID, NodeID destID, int r,
								   Edge **edges) {
	ASSERT(g && r < Graph_RelationTypeCount(g) && edges);

	// Invalid relation type specified; this can occur on multi-type traversals like:
	// MATCH ()-[:real_type|fake_type]->()
	if(r == GRAPH_UNKNOWN_RELATION) return;

	Node srcNode = GE_NEW_NODE();
	Node destNode = GE_NEW_NODE();
	ASSERT(Graph_GetNode(g, srcID, &srcNode));
	ASSERT(Graph_GetNode(g, destID, &destNode));

	if(r != GRAPH_NO_RELATION) {
		_Graph_GetEdgesConnectingNodes(g, srcID, destID, r, edges);
	} else {
		// Relation type missing, scan through each edge type.
		int relationCount = Graph_RelationTypeCount(g);
		for(int i = 0; i < relationCount; i++) {
			_Graph_GetEdgesConnectingNodes(g, srcID, destID, i, edges);
		}
	}
}

uint64_t Graph_BulkCreateNodes(Graph *g, const int label, const uint64_t n_to_alloc)
/* Allocate n_to_alloc new nodes with label in the graph.  This
   returns the starting node id and assumes sequential allocation of
   ids.
 */
{
	ASSERT(g);

        DataBlock *nodes = g->nodes;
        const uint64_t start_id = DataBlock_BulkAllocateItems (nodes, n_to_alloc);

        for (uint64_t k = start_id; k < start_id + n_to_alloc; ++k) {
             Entity *en = DataBlock_GetItem (nodes, k);
             en->prop_count = 0;
             en->properties = NULL;
        }

        if (label != GRAPH_NO_LABEL) {
             const GrB_Index dims = Graph_RequiredMatrixDim(g);
             RG_Matrix matrix = g->labels[label];
             _MatrixResizeToCapacity(g, matrix);
             GrB_Matrix m = RG_Matrix_Get_GrB_Matrix(matrix);

             GrB_Info info;
             UNUSED(info);

             GrB_Index *I;
             bool *X;
             I = rm_malloc (n_to_alloc * sizeof (*I));
             ASSERT(I != NULL);
             X = rm_malloc (n_to_alloc * sizeof (*X));
             ASSERT(X != NULL);
             for (uint64_t k = 0; k < n_to_alloc; ++k) {
                  I[k] = k;
                  X[k] = true;
             }

             GrB_Matrix diag;
             info = GrB_Matrix_new (&diag, GrB_BOOL, n_to_alloc, n_to_alloc);
             ASSERT(info == GrB_SUCCESS);
             info = GrB_Matrix_build (diag, I, I, X, n_to_alloc, GxB_ANY_BOOL);
             ASSERT(info == GrB_SUCCESS);
             for (uint64_t k = 0; k < n_to_alloc; ++k) I[k] += start_id;
             info = GrB_assign (m, GrB_NULL, GrB_NULL, diag, I, n_to_alloc, I, n_to_alloc, GrB_NULL);
             ASSERT(info == GrB_SUCCESS);
             GrB_free (&diag);
             rm_free (X);
             rm_free (I);
        }

        return start_id;
}

void Graph_CreateNode(Graph *g, int label, Node *n) {
	ASSERT(g);

	NodeID id;
	Entity *en = DataBlock_AllocateItem(g->nodes, &id);
	n->id = id;
	n->entity = en;
	en->prop_count = 0;
	en->properties = NULL;

	if(label != GRAPH_NO_LABEL) {
		// Try to set matrix at position [id, id]
		// incase of a failure, scale matrix.
		RG_Matrix matrix = g->labels[label];
		GrB_Matrix m = RG_Matrix_Get_GrB_Matrix(matrix);
		GrB_Info res = GrB_Matrix_setElement_BOOL(m, true, id, id);
		if(res != GrB_SUCCESS) {
			_MatrixResizeToCapacity(g, matrix);
			res = GrB_Matrix_setElement_BOOL(m, true, id, id);
			ASSERT(res == GrB_SUCCESS);
		}
	}
}

static void add_relation_id (EdgeID* z_out, EdgeID x_in, EdgeID y_newid)
{
    EdgeID *ids;
    /* Single edge ID,
     * switching from single edge ID to multiple IDs. */
    if(SINGLE_EDGE(x_in)) {
        ids = array_new(EdgeID, 2);
        ids = array_append(ids, SINGLE_EDGE_ID(x_in));
        ids = array_append(ids, SINGLE_EDGE_ID(y_newid));
        // TODO: Make sure MSB of ids isn't on.
        *z_out = (EdgeID)ids;
    } else {
        // Multiple edges, adding another edge.
        ids = (EdgeID *)(x_in);
        ids = array_append(ids, SINGLE_EDGE_ID(y_newid));
        *z_out = (EdgeID)ids;
    }
}

static void combine_relation_ids (EdgeID* z_out, EdgeID x_in, EdgeID y_in)
/*
  z_out becomes the merger of x_in and y_in.  If one or the other is
  an array, that array is extended.  If both are arrays, then x_in is
  extended and y_in is freed.

  NOTE: Ensure that y_in is not used again in calling code.
 */
{
    EdgeID *ids;
    /* Single edge ID,
     * switching from single edge ID to multiple IDs. */
    if (SINGLE_EDGE(x_in) && SINGLE_EDGE(y_in)) {
        ids = array_new(EdgeID, 2);
        ids = array_append(ids, SINGLE_EDGE_ID(x_in));
        ids = array_append(ids, SINGLE_EDGE_ID(y_in));
        *z_out = (EdgeID)ids;
    } else if ((!(SINGLE_EDGE(x_in)) && SINGLE_EDGE(y_in))) {
        // Multiple edges, adding another edge.
        ids = (EdgeID *)(x_in);
        ids = array_append(ids, SINGLE_EDGE_ID(y_in));
        *z_out = (EdgeID)ids;
    } else if ((SINGLE_EDGE(x_in) && !(SINGLE_EDGE(y_in)))) {
        // Multiple edges, adding another edge.
        ids = (EdgeID *)(y_in);
        ids = array_append(ids, SINGLE_EDGE_ID(x_in));
        *z_out = (EdgeID)ids;
    } else {
         ids = (EdgeID*)x_in;
         array_ensure_append (ids, (EdgeID*)y_in, array_len((EdgeID*)y_in), EdgeID);
         *z_out = (EdgeID)ids;
         array_free ((EdgeID*)y_in);
    }
}

/*
  XXX MULTI-EDGE-PROBLEM: Non-multi-edge graphs are not treated
  uniformly.  FormConnection handles them, but ConnectNodes returns 1
  even if the edge already is there.  And which id overwrites which,
  what to do with the old edge id, etc. is undefined.
 */
void Graph_FormConnection(Graph *g, NodeID src, NodeID dest, EdgeID edge_id, int r) {
	GrB_Info info;
	UNUSED(info);
	RG_Matrix M = g->relations[r];
	GrB_Matrix t_relationMat = NULL;
	GrB_Matrix adj = Graph_GetAdjacencyMatrix(g);
	GrB_Matrix tadj = Graph_GetTransposedAdjacencyMatrix(g);
	GrB_Matrix relationMat = Graph_GetRelationMatrix(g, r);

	bool maintain_transpose;
	Config_Option_get(Config_MAINTAIN_TRANSPOSE, &maintain_transpose);
	if(maintain_transpose) {
		t_relationMat = Graph_GetTransposedRelationMatrix(g, r);
	}

	// Rows represent source nodes, columns represent destination nodes.
	edge_id = SET_MSB(edge_id);
	info = GrB_Matrix_setElement_BOOL(adj, true, src, dest); ASSERT(info == GrB_SUCCESS);
	info = GrB_Matrix_setElement_BOOL(tadj, true, dest, src); ASSERT(info == GrB_SUCCESS);

	// Matrix multi-edge is enable for this matrix, use GxB_Matrix_subassign.
	if(_RG_Matrix_MultiEdgeEnabled(M)) {
		GrB_Index I = src;
		GrB_Index J = dest;
                EdgeID relationMat_ij = (EdgeID)-1;
                info = GrB_Matrix_extractElement_UINT64 ((uint64_t*)&relationMat_ij, relationMat, src, dest);
                if (info == GrB_NO_VALUE) {
                     info = GrB_Matrix_setElement_UINT64 (relationMat, (uint64_t)edge_id, src, dest);
                     assert(info == GrB_SUCCESS);
                } else {
                     assert (info == GrB_SUCCESS);
                     add_relation_id (&relationMat_ij, relationMat_ij, edge_id);
                     info = GrB_Matrix_setElement_UINT64 (relationMat, (uint64_t)relationMat_ij, src, dest);
                     assert(info == GrB_SUCCESS);
                }

		// Update the transposed matrix if one is present.
		if(t_relationMat != NULL) {
                     info = GrB_Matrix_extractElement_UINT64 ((uint64_t*)&relationMat_ij, t_relationMat, dest, src);
                     if (info == GrB_NO_VALUE) {
                          info = GrB_Matrix_setElement_UINT64 (t_relationMat, (uint64_t)edge_id, dest, src);
                          assert(info == GrB_SUCCESS);
                     } else {
                          assert (info == GrB_SUCCESS);
                          add_relation_id (&relationMat_ij, relationMat_ij, edge_id);
                          info = GrB_Matrix_setElement_UINT64 (t_relationMat, (uint64_t)relationMat_ij, dest, src);
                          assert(info == GrB_SUCCESS);
                     }
                }
	} else {
		// Multi-edge is disabled, use GrB_Matrix_setElement.
		info = GrB_Matrix_setElement_UINT64(relationMat, edge_id, src, dest);
		ASSERT(info == GrB_SUCCESS);

		// Update the transposed matrix if one is present.
		if(t_relationMat != NULL) {
			info = GrB_Matrix_setElement_UINT64(t_relationMat, edge_id, dest, src);
			ASSERT(info == GrB_SUCCESS);
		}
	}
}

int Graph_ConnectNodes(Graph *g, NodeID src, NodeID dest, int r, Edge *e) {
	Node srcNode = GE_NEW_NODE();
	Node destNode = GE_NEW_NODE();

	int res;
	UNUSED(res);
	res = Graph_GetNode(g, src, &srcNode);
	ASSERT(res == 1);
	res = Graph_GetNode(g, dest, &destNode);
	ASSERT(res == 1);
	ASSERT(g && r < Graph_RelationTypeCount(g));

	EdgeID id;
	Entity *en = DataBlock_AllocateItem(g->edges, &id);
	en->prop_count = 0;
	en->properties = NULL;
	e->id = id;
	e->entity = en;
	e->relationID = r;
	e->srcNodeID = src;
	e->destNodeID = dest;
	Graph_FormConnection(g, src, dest, id, r);
	return 1;
}

EdgeID Graph_BulkConnectNodes(Graph *g, NodeID *I, NodeID *J, const size_t n_new_edges, int r)
/* Add n_new_edges into graph g with relation id r.  This returns the
   starting edge id and assumes sequential allocation of ids.  Returns
   (EdgeID)-1 if no new edges were created, which never occurs when
   the relation supports multi-edges.

   NOTE:  src, dest may not be unique in this call or across calls.
 */
{
     EdgeID out;
     /*
       1. Pre-allocate edge records.
       2. Find duplicate edges in the *input*, but do not merge yet.
       3. Update the adjacency matrices.
       4. Update the existing relation matrix if it exists.
          4a. Merge the input and existing id arrays.
          4b. Uniq / compress the [I,J,id] entries.
          4c. If multi-graph is enabled, merge duplicate id arrays. If
          not, ensure previous id datablock entries are marked deleted
          and assign new ones.

       Edge properties are handled in the *calling* routine.
      */

     DataBlock *edges = g->edges;
     const uint64_t start_id = DataBlock_BulkAllocateItems (edges, n_new_edges);
     uint64_t end_id = start_id + n_new_edges;
     for (uint64_t k = start_id; k < start_id + n_new_edges; ++k) {
          Entity *en = DataBlock_GetItem (edges, k);
          en->prop_count = 0;
          en->properties = NULL;
     }

     /* Get the existing relation matrix and ensure it's of sufficient
        size.  Moved here to check if multi-edge is enabled. */
     ASSERT(r != GRAPH_NO_RELATION);

     RG_Matrix matrix = g->relations[r];
     _MatrixResizeToCapacity(g, matrix);
     const bool multi_edge = _RG_Matrix_MultiEdgeEnabled (matrix);

     /*
        Find duplicate entries in the *input*.

        Merges happen into earlier records, so the second argument
        always is a single edge and add_relation_id works.
      */

     rax *rt = raxNew();
     EdgeID *all_ids = rm_malloc (n_new_edges * sizeof (*all_ids));
     for (uint64_t k = 0; k < n_new_edges; ++k)
          all_ids[k] = SET_MSB(start_id + k);

     size_t n_uniq_new_edges = n_new_edges;
     for (size_t k = 0; k < n_new_edges; ++k) {
          int lookup;
          EdgeID cur_id = all_ids[k];
          EdgeID *old_id_ptr = NULL;
          GrB_Index IJ[2] = { I[k], J[k] };

          lookup = raxTryInsert (rt, (unsigned char*)IJ, sizeof(IJ), &all_ids[k], (void**)&old_id_ptr);
          if (lookup == 0) {
               if (multi_edge) {
                    add_relation_id (old_id_ptr, *old_id_ptr, cur_id);
                    if (!(SINGLE_EDGE(cur_id))) array_free ((EdgeID*)cur_id);
               } else {
                    DataBlock_DeleteItem (edges, cur_id);
               }
               all_ids[k] = (EdgeID)-1; // Mark as a duplicate.
               --n_uniq_new_edges;
          }
     }

     /* Create and union in the adjacency matrices.  Matrices
        already have been resized.  Now build for the full size and eWiseAdd. */
     GrB_Matrix addl_adj = GrB_NULL;  /* Used as a mask later. */
     GrB_Index nrows;

     {
          GrB_Matrix adj = Graph_GetAdjacencyMatrix (g);
          GrB_Matrix tadj = Graph_GetTransposedAdjacencyMatrix (g);
          GrB_Info info;
          UNUSED(info);

          info = GrB_Matrix_nrows (&nrows, adj);
          ASSERT (info == GrB_SUCCESS);

          bool *X = rm_malloc (n_new_edges * sizeof (*X));
          ASSERT (X != NULL);
          for (GrB_Index k = 0; k < n_new_edges; ++k) X[k] = true;

          info = GrB_Matrix_new (&addl_adj, GrB_BOOL, nrows, nrows);
          info = GrB_Matrix_build (addl_adj, I, J, X, n_new_edges, GxB_PAIR_BOOL);
          ASSERT (info == GrB_SUCCESS);

          info = GrB_eWiseAdd (adj, GrB_NULL, GrB_NULL, GrB_LOR, adj, addl_adj, GrB_NULL);
          ASSERT (info == GrB_SUCCESS);
          info = GrB_eWiseAdd (tadj, GrB_NULL, GrB_NULL, GrB_LOR, tadj, addl_adj, GrB_DESC_T1);
          ASSERT (info == GrB_SUCCESS);

          rm_free (X);
     }

     /* Find duplicate, pre-existing edges for the relation matrix.
        New relation matrices are built in the header reader in
        bulk_insert.c's routines, so this routine cannot assume the
        matrix is empty.  Another "file" in a GRAPH.CREATE call may
        have added entries.

        Note: If a GraphBLAS implementation library properly
        supports the dup GrB_BinaryOp parameter in
        GrB_Matrix_build, *AND* if the GraphBLAS routines can
        manipulate both the host memory and the GraphBLAS memory,
        that operation could extend the id arrays itself through a
        user-defined function.  Managing memory out-of-band that
        way is a bit tricky if the build routine is parallel.

        The current implementation does not assume that the host and
        GraphBLAS can manage each others' memory, e.g. GraphBLAS is
        on an accelerator.
     */

     GrB_Matrix relmat = RG_Matrix_Get_GrB_Matrix (matrix);
     GrB_Matrix relmat_slice = GrB_NULL;
     GrB_Info info;
     UNUSED(info);

     info = GrB_Matrix_new (&relmat_slice, GrB_UINT64, nrows, nrows);
     ASSERT (info == GrB_SUCCESS);
     // Extract through a mask retrieving only changed entries.
     info = GrB_Matrix_extract (relmat_slice, addl_adj, GrB_NULL, relmat,
                                GrB_ALL, nrows, GrB_ALL, nrows, GrB_NULL);
     ASSERT (info == GrB_SUCCESS);

     GrB_Index old_relmat_nvals;
     info = GrB_Matrix_nvals (&old_relmat_nvals, relmat_slice);
     ASSERT (info == GrB_SUCCESS);

     if (old_relmat_nvals) { // Hopefully unlikely.
          // Merge in any existing edge ids.
          // Extract and insert into the hash table.
          GrB_Index *old_I;
          GrB_Index *old_J;
          uint64_t *old_X;
          GrB_Index actually_extracted;

          old_I = rm_malloc (2 * old_relmat_nvals * sizeof (*old_I));
          old_J = &old_I[old_relmat_nvals];
          old_X = rm_malloc (old_relmat_nvals * sizeof (*old_X));
          info = GrB_Matrix_extractTuples (old_I, old_J, old_X, &actually_extracted, relmat_slice);
          ASSERT (info == GrB_SUCCESS);
          ASSERT (actually_extracted == old_relmat_nvals);

          if(_RG_Matrix_MultiEdgeEnabled(matrix)) {
               for (size_t k = 0; k < old_relmat_nvals; ++k) {
                    int lookup;
                    EdgeID old_id = old_X[k];
                    EdgeID *new_id_ptr;
                    GrB_Index IJ[2] = { old_I[k], old_J[k] };

                    new_id_ptr = raxFind (rt, (unsigned char*)IJ, sizeof(IJ));
                    ASSERT (new_id_ptr != raxNotFound); // Extracted through the addl_adj = [I,J] mask.
                    combine_relation_ids (new_id_ptr, *new_id_ptr, old_id); // invalidates old_id
               }
          } else {
               // Caveat: See comment marked MULTI-EDGE-PROBLEM above.
               // Over-write existing edges and mark the overlapping
               // newly allocated records as deleted.
               for (size_t k = 0; k < old_relmat_nvals; ++k) {
                    int lookup;
                    EdgeID old_id = old_X[k];
                    EdgeID *new_id_ptr;
                    GrB_Index IJ[2] = { old_I[k], old_J[k] };

                    new_id_ptr = raxFind (rt, (unsigned char*)IJ, sizeof(IJ));
                    ASSERT (new_id_ptr != raxNotFound); // Extracted through the addl_adj = [I,J] mask.
                    --n_uniq_new_edges;
                    ASSERT((SINGLE_EDGE(*new_id_ptr)));
                    DataBlock_DeleteItem (edges, *new_id_ptr);
               }
          }
          rm_free (old_X);
          rm_free (old_I);
     }
     // No longer need the hash table, and it will be incorrect shortly.
     raxFree (rt);
     rt = NULL;

     if (n_uniq_new_edges == 0) {
          out = (EdgeID)-1;
          goto cleanup;
     }

     // Compress the I, J, all_ids down to unique edges.
     uint64_t n_uniq_edges = 0;
     {
          size_t first = 0;
          do {
               if (all_ids[first] != -1) {
                    I[n_uniq_edges] = I[first];
                    J[n_uniq_edges] = J[first];
                    all_ids[n_uniq_edges] = all_ids[first];
                    ++n_uniq_edges;
               }
          } while (++first < n_new_edges);
     }
     ASSERT (n_uniq_edges > 0); // Utter paranoia.

     // Build the matrix to be inserted.
     info = GrB_Matrix_clear (relmat_slice);
     ASSERT (info == GrB_SUCCESS);
     // No duplicates left, but pick the first just in case.
     info = GrB_Matrix_build (relmat_slice, I, J, all_ids, n_uniq_edges, GrB_FIRST_UINT64);
     ASSERT (info == GrB_SUCCESS);

     // At long last, replace the entries.  These could be GrB_assign through an addl_adj mask.
     info = GrB_eWiseAdd (relmat, GrB_NULL, GrB_NULL, GrB_SECOND_UINT64, relmat, relmat_slice, GrB_NULL);
     bool maintain_transpose;
     Config_Option_get(Config_MAINTAIN_TRANSPOSE, &maintain_transpose);
     if (maintain_transpose) { // Creation, so assume symmetric...
          RG_Matrix t_matrix = g->t_relations[r];
          g->SynchronizeMatrix(g, t_matrix);
          GrB_Matrix t_relmat = RG_Matrix_Get_GrB_Matrix (t_matrix);
          info = GrB_eWiseAdd (t_relmat, GrB_NULL, GrB_NULL, GrB_SECOND_UINT64, t_relmat,
                               relmat_slice, GrB_DESC_T1);
     }

     out = start_id;
cleanup:
     GrB_free (&relmat_slice);
     GrB_free (&addl_adj);
     return out;
}

static void
gather_edges (const Graph *g, GrB_Vector v, const GrB_Index vtx, const GRAPH_EDGE_DIR dir,
              const int edgeType, Edge **edges_in, GrB_Index **neigh_in, uint64_t **ids_in)
{
     Edge *edges = *edges_in;
     GrB_Index *neigh = *neigh_in;
     uint64_t *ids = *ids_in;
     GrB_Index nvals;
     GrB_Info info = GrB_Vector_nvals (&nvals, v); UNUSED(info);
     ASSERT (info == GrB_SUCCESS);

     ASSERT (array_len (neigh) == 0 && array_len (ids) == 0);

     ids = array_grow (ids, nvals);
     neigh = array_grow (neigh, nvals);
     ASSERT (ids != NULL && neigh != NULL);

     GrB_Index nvals_saved = nvals; UNUSED(nvals_saved);
     GrB_Vector_extractTuples (neigh, ids, &nvals, v);
     ASSERT (nvals_saved == nvals);

     Edge e;
     if (GRAPH_EDGE_DIR_OUTGOING == dir)
          e.srcNodeID = vtx;
     else
          e.destNodeID = vtx;
     e.relationID = edgeType;
     for (size_t k = 0; k < nvals; ++k) {
          GrB_Index dest = neigh[k];
          EdgeID edge_id = ids[k];

          if (GRAPH_EDGE_DIR_OUTGOING == dir)
               e.destNodeID = dest;
          else
               e.srcNodeID = dest;

          if (SINGLE_EDGE(edge_id)) {
               edge_id = SINGLE_EDGE_ID(edge_id);
               e.id = edge_id;
               e.entity = DataBlock_GetItem (g->edges, edge_id);
               ASSERT(e.entity);
               edges = array_append (edges, e);
          } else {
               EdgeID* edge_ids = (EdgeID*)edge_id;
               size_t nedges = array_len (edge_ids);
               for (size_t idk = 0; idk < nedges; ++idk) {
                    e.id = edge_ids[idk];
                    e.entity = DataBlock_GetItem (g->edges, e.id);
                    ASSERT(e.entity);
                    edges = array_append (edges, e);
               }
          }
     }

     array_clear (neigh); *neigh_in = neigh;
     array_clear (ids); *ids_in = ids;
     *edges_in = edges;
}

/* Retrieves all either incoming or outgoing edges
 * to/from given node N, depending on given direction. */
void Graph_GetNodeEdges(const Graph *g, const Node *n, GRAPH_EDGE_DIR dir, int edgeType,
                        Edge **edges) {
	ASSERT(g && n && edges);
        GrB_Index *neigh = NULL;
        uint64_t *ids = NULL;
        const NodeID vtx = ENTITY_GET_ID(n);

	if(edgeType == GRAPH_UNKNOWN_RELATION) return;

        neigh = array_new (GrB_Index, 32);
        ids = array_new (uint64_t, 32);
        ASSERT (ids != NULL && neigh != NULL);

	bool maintain_transpose;
	Config_Option_get(Config_MAINTAIN_TRANSPOSE, &maintain_transpose);

        if (edgeType != GRAPH_NO_RELATION) {
             GrB_Matrix R = Graph_GetRelationMatrix(g, edgeType);
             GrB_Index N;
             GrB_Vector v = GrB_NULL;
             GrB_Info info; UNUSED(info);

             info = GrB_Matrix_ncols (&N, R);
             ASSERT (info == GrB_SUCCESS);
             info = GrB_Vector_new (&v, GrB_UINT64, N);
             ASSERT (info == GrB_SUCCESS);
             if (dir == GRAPH_EDGE_DIR_OUTGOING || dir == GRAPH_EDGE_DIR_BOTH) {
                  info = GrB_extract (v, GrB_NULL, GrB_NULL, R, GrB_ALL, N, vtx, GrB_DESC_RT0);
                  ASSERT (info == GrB_SUCCESS);
                  gather_edges (g, v, vtx, GRAPH_EDGE_DIR_OUTGOING, edgeType, edges, &neigh, &ids);
             }
             if (dir == GRAPH_EDGE_DIR_INCOMING || dir == GRAPH_EDGE_DIR_BOTH) {
                  if (maintain_transpose) {
                       GrB_Matrix Rt = Graph_GetTransposedRelationMatrix(g, edgeType);
                       info = GrB_extract (v, GrB_NULL, GrB_NULL, Rt, GrB_ALL, N, vtx, GrB_DESC_RT0);
                  } else
                       info = GrB_extract (v, GrB_NULL, GrB_NULL, R, GrB_ALL, N, vtx, GrB_DESC_R);
                  ASSERT (info == GrB_SUCCESS);
                  gather_edges (g, v, vtx, GRAPH_EDGE_DIR_INCOMING, edgeType, edges, &neigh, &ids);
             }
             GrB_free (&v);
        } else {
             GrB_Matrix adj = Graph_GetAdjacencyMatrix (g);
             GrB_Vector v = GrB_NULL;
             GrB_Index N;
             GrB_Info info; UNUSED(info);
             const int relationCount = Graph_RelationTypeCount(g);

             info = GrB_Matrix_ncols (&N, adj);
             ASSERT (info == GrB_SUCCESS);
             info = GrB_Vector_new (&v, GrB_UINT64, N);
             ASSERT (info == GrB_SUCCESS);

             if (dir == GRAPH_EDGE_DIR_OUTGOING || dir == GRAPH_EDGE_DIR_BOTH) {
                  for(int i = 0; i < relationCount; i++) {
                       GrB_Matrix R = Graph_GetRelationMatrix (g, i);
                       info = GrB_extract (v, GrB_NULL, GrB_NULL, R, GrB_ALL, N, vtx, GrB_DESC_RT0);
                       ASSERT (info == GrB_SUCCESS);
                       gather_edges (g, v, vtx, GRAPH_EDGE_DIR_OUTGOING, i, edges, &neigh, &ids);
                  }
             }
             if (dir == GRAPH_EDGE_DIR_INCOMING || dir == GRAPH_EDGE_DIR_BOTH) {
                  for(int i = 0; i < relationCount; i++) {
                       if (maintain_transpose) {
                            GrB_Matrix Rt = Graph_GetTransposedRelationMatrix(g, i);
                            info = GrB_extract (v, GrB_NULL, GrB_NULL, Rt, GrB_ALL, N, vtx, GrB_DESC_RT0);
                       } else {
                            GrB_Matrix R = Graph_GetRelationMatrix(g, i);
                            info = GrB_extract (v, GrB_NULL, GrB_NULL, R, GrB_ALL, N, vtx, GrB_DESC_R);
                       }
                       ASSERT (info == GrB_SUCCESS);
                       gather_edges (g, v, vtx, GRAPH_EDGE_DIR_INCOMING, i, edges, &neigh, &ids);
                  }
             }
             GrB_free (&v);
        }
        array_free (neigh);
        array_free (ids);
}

/* Removes an edge from Graph and updates graph relevent matrices. */
int Graph_DeleteEdge(Graph *g, Edge *e) {
	uint64_t x;
	GrB_Matrix R;
	GrB_Matrix M;
	GrB_Info info;
	EdgeID edge_id;
	GrB_Matrix TR = GrB_NULL;
	int r = Edge_GetRelationID(e);
	NodeID src_id = Edge_GetSrcNodeID(e);
	NodeID dest_id = Edge_GetDestNodeID(e);

	R = Graph_GetRelationMatrix(g, r);
	bool maintain_transpose;
	Config_Option_get(Config_MAINTAIN_TRANSPOSE, &maintain_transpose);
	if(maintain_transpose) TR = Graph_GetTransposedRelationMatrix(g, r);

	// Test to see if edge exists.
	info = GrB_Matrix_extractElement(&edge_id, R, src_id, dest_id);
	if(info != GrB_SUCCESS) return 0;

	if(SINGLE_EDGE(edge_id)) {
		// Single edge of type R connecting src to dest, delete entry.
		info = GxB_Matrix_Delete(R, src_id, dest_id);
		ASSERT(info == GrB_SUCCESS);
		if(TR) {
			info = GxB_Matrix_Delete(TR, dest_id, src_id);
			ASSERT(info == GrB_SUCCESS);
		}

		// See if source is connected to destination with additional edges.
		bool connected = false;
		int relationCount = Graph_RelationTypeCount(g);
		for(int i = 0; i < relationCount; i++) {
			if(i == r) continue;
			M = Graph_GetRelationMatrix(g, i);
			info = GrB_Matrix_extractElement(&x, M, src_id, dest_id);
			if(info == GrB_SUCCESS) {
				connected = true;
				break;
			}
		}

		/* There are no additional edges connecting source to destination
		 * Remove edge from THE adjacency matrix. */
		if(!connected) {
			M = Graph_GetAdjacencyMatrix(g);
			info = GxB_Matrix_Delete(M, src_id, dest_id);
			ASSERT(info == GrB_SUCCESS);

			M = Graph_GetTransposedAdjacencyMatrix(g);
			info = GxB_Matrix_Delete(M, dest_id, src_id);
			ASSERT(info == GrB_SUCCESS);
		}
	} else {
		/* Multiple edges connecting src to dest
		 * locate specific edge and remove it
		 * revert back from array representation to edge ID
		 * incase we're left with a single edge connecting src to dest. */

		int i = 0;
		EdgeID id = ENTITY_GET_ID(e);
		EdgeID *edges = (EdgeID *)edge_id;
		int edge_count = array_len(edges);

		// Locate edge within edge array.
		for(; i < edge_count; i++) if(edges[i] == id) break;
		ASSERT(i < edge_count);

		/* Remove edge from edge array
		 * migrate last edge ID and reduce array size.
		 * TODO: reallocate array of size / capacity ratio is high. */
		edges[i] = edges[edge_count - 1];
		array_pop(edges);

		/* Incase we're left with a single edge connecting src to dest
		 * revert back from array to scalar. */
		if(array_len(edges) == 1) {
			edge_id = edges[0];
			array_free(edges);
			GrB_Matrix_setElement(R, SET_MSB(edge_id), src_id, dest_id);
		}

		if(TR) {
			/* We must make the matching updates to the transposed matrix.
			 * First, extract the element that is known to be an edge array. */
			info = GrB_Matrix_extractElement(edges, TR, dest_id, src_id);
			ASSERT(info == GrB_SUCCESS);
			// Replace the deleted edge with the last edge in the matrix.
			edges[i] = edges[edge_count - 1];
			array_pop(edges);
			// Free and replace the array if it now has 1 element.
			if(array_len(edges) == 1) {
				edge_id = edges[0];
				array_free(edges);
				GrB_Matrix_setElement(TR, SET_MSB(edge_id), dest_id, src_id);
			}
		}
	}

	// Free and remove edges from datablock.
	DataBlock_DeleteItem(g->edges, ENTITY_GET_ID(e));
	return 1;
}

void Graph_DeleteNode(Graph *g, Node *n) {
	/* Assumption, node is completely detected,
	 * there are no incoming nor outgoing edges
	 * leading to / from node. */
	ASSERT(g && n);

	// Clear label matrix at position node ID.
	uint32_t label_count = array_len(g->labels);
	for(int i = 0; i < label_count; i++) {
		GrB_Matrix M = Graph_GetLabelMatrix(g, i);
		GxB_Matrix_Delete(M, ENTITY_GET_ID(n), ENTITY_GET_ID(n));
	}

	DataBlock_DeleteItem(g->nodes, ENTITY_GET_ID(n));
}

static void free_edge (Graph *g, EdgeID id)
{
     if (SINGLE_EDGE(id)) {
          DataBlock_DeleteItem (g->edges, SINGLE_EDGE_ID(id));
     } else {
          EdgeID *ids = (EdgeID *) id;
          //if (*ids == NULL) return;
          uint id_count = array_len(ids);
          for (uint i = 0; i < id_count; i++) DataBlock_DeleteItem(g->edges, ids[i]);
          if (id_count > 0) array_free(ids);
          //*ids = NULL;
     }
}

static void _Graph_FreeRelationMatrixEdges(Graph *g, GrB_Matrix grb_matrix, EdgeID **valptr)
{
        EdgeID *val = *valptr;

        GrB_Index nvals, nvals_saved;
        UNUSED(nvals_saved);
        GrB_Info info;
        UNUSED(info);

        info = GrB_Matrix_nvals (&nvals, grb_matrix);
        ASSERT(info == GrB_SUCCESS);
        if (nvals == 0) return;
        if (!val)
             val = array_newlen (EdgeID, nvals);
        else
             val = array_ensure_len (val, nvals);
        ASSERT(val);
        *valptr = val;

#if !defined(NDEBUG)||defined(RG_DEBUG)
        nvals_saved = nvals;
#endif

        info = GrB_Matrix_extractTuples (GrB_NULL, GrB_NULL, val, &nvals, grb_matrix);
        ASSERT(info == GrB_SUCCESS);
        ASSERT(nvals == nvals_saved);

        for (GrB_Index k = 0; k < nvals; ++k)
             free_edge (g, val[k]);
        array_clear (val);
}

static void _Graph_FreeRelationMatrices(Graph *g) {
	uint relationCount = Graph_RelationTypeCount(g);
        EdgeID *val = NULL;
	bool maintain_transpose;
	Config_Option_get(Config_MAINTAIN_TRANSPOSE, &maintain_transpose);
	for(uint i = 0; i < relationCount; i++) {
		RG_Matrix M = g->relations[i];

                GrB_Matrix grb_matrix = M->grb_matrix;
                _Graph_FreeRelationMatrixEdges (g, grb_matrix, &val);

		// Free the matrix itself.
		RG_Matrix_Free(M);

		// Perform the same update to transposed matrices.
		if(maintain_transpose) {
			RG_Matrix TM = g->t_relations[i];

                        grb_matrix = TM->grb_matrix;
                        _Graph_FreeRelationMatrixEdges (g, grb_matrix, &val);

			// Free the matrix itself.
			RG_Matrix_Free(TM);
		}
	}
        array_free (val);
}

#define is_GrB_Index_lt(a,b) ((*a)<(*b))

static void _BulkDeleteNodes(Graph *g, Node *nodes, uint node_count,
							 uint *node_deleted, uint *edge_deleted) {
	ASSERT(g && g->_writelocked && nodes && node_count > 0);

        GrB_Info info;
        UNUSED(info);
        GrB_Matrix tmp;
        GrB_Index dim, nrows;

	GrB_Matrix adj;                     // Adjacency matrix.
	GrB_Matrix tadj;                    // Transposed adjacency matrix.

        GrB_Index *node_idx = NULL;

        GrB_wait ();

	adj = Graph_GetAdjacencyMatrix(g);
	tadj = Graph_GetTransposedAdjacencyMatrix(g);

        dim = Graph_RequiredMatrixDim (g);
        GrB_Matrix_nrows (&nrows, adj);

        node_idx = rm_malloc (node_count * sizeof (*node_idx));
        ASSERT(node_idx);
        for(uint i = 0; i < node_count; i++)
             node_idx[i] = ENTITY_GET_ID(nodes + i);

        // Removing duplicates.
        QSORT(GrB_Index, node_idx, node_count, is_GrB_Index_lt);

        {
             size_t uniqueIdx = 0;
             for(int i = 0; i < node_count; i++) {
                  if (node_idx[i] >= nrows) break;
                  // As long as current is the same as follows.
                  while(i < node_count - 1 && node_idx[i] == node_idx[i+1]) i++;

                  if(uniqueIdx < i) node_idx[uniqueIdx] = node_idx[i];
                  uniqueIdx++;
             }
             node_count = uniqueIdx;
        }
        *node_deleted = node_count;

        // Build an empty matrix to clear the rows.
        info = GrB_Matrix_new (&tmp, GrB_UINT64, node_count, dim);
        ASSERT(info == GrB_SUCCESS);

        // Clear the row nodes first (both adj and tadj).  Should be microscopically faster for CSR.
        info = GrB_assign (adj, GrB_NULL, GrB_NULL, tmp, node_idx, node_count, GrB_ALL, dim,
                           GrB_NULL);
        ASSERT(info == GrB_SUCCESS);
        info = GrB_assign (tadj, GrB_NULL, GrB_NULL, tmp, node_idx, node_count, GrB_ALL, dim,
                           GrB_NULL);
        ASSERT(info == GrB_SUCCESS);

        // Clear the columns of both second, should be more sparse now.
        info = GrB_assign (adj, GrB_NULL, GrB_NULL, tmp, GrB_ALL, dim, node_idx, node_count,
                           GrB_DESC_T0);
        ASSERT(info == GrB_SUCCESS);
        info = GrB_assign (tadj, GrB_NULL, GrB_NULL, tmp, GrB_ALL, dim, node_idx, node_count,
                           GrB_DESC_T0);
        ASSERT(info == GrB_SUCCESS);

        /* // FIXME: This computation isn't right. Should be 2 nodes and 4 edges deleted. */
	/* *node_deleted += DataBlock_DeletedItemsCount (g->nodes); */
	/* *edge_deleted += DataBlock_DeletedItemsCount (g->edges); */

	// Free and remove implicit edges from relation matrices.
        EdgeID *val = NULL;
	const int relation_count = Graph_RelationTypeCount(g);
        bool update_transpose;
	Config_Option_get(Config_MAINTAIN_TRANSPOSE, &update_transpose);
	for(int i = 0; i < relation_count; i++) {
		GrB_Matrix R = Graph_GetRelationMatrix(g, i);
                GrB_Matrix Rt = GrB_NULL;
                if (update_transpose) Rt = Graph_GetTransposedRelationMatrix(g, i);

                info = GxB_Matrix_resize (tmp, node_count, dim);
                ASSERT(info == GrB_SUCCESS);

                info = GrB_extract (tmp, GrB_NULL, GrB_NULL, R, node_idx, node_count, GrB_ALL, dim,
                                    GrB_NULL);
                ASSERT(info == GrB_SUCCESS);
                _Graph_FreeRelationMatrixEdges (g, tmp, &val);
                GrB_Matrix_clear (tmp);
                info = GrB_assign (R, GrB_NULL, GrB_NULL, tmp, node_idx, node_count, GrB_ALL, dim,
                                   GrB_NULL);
                ASSERT(info == GrB_SUCCESS);

                info = GrB_extract (tmp, GrB_NULL, GrB_NULL, R, GrB_ALL, dim, node_idx, node_count,
                                    GrB_DESC_T1);
                ASSERT(info == GrB_SUCCESS);
                _Graph_FreeRelationMatrixEdges (g, tmp, &val);
                GrB_Matrix_clear (tmp);
                info = GrB_assign (R, GrB_NULL, GrB_NULL, tmp, GrB_ALL, dim, node_idx, node_count,
                                   GrB_DESC_T0);
                ASSERT(info == GrB_SUCCESS);

                if (update_transpose) {
                     {
                          GrB_Index nv;
                          info = GrB_extract (tmp, GrB_NULL, GrB_NULL, Rt, node_idx, node_count, GrB_ALL, dim,
                                              GrB_NULL);
                          ASSERT(info == GrB_SUCCESS);
                          GrB_Matrix_nvals (&nv, tmp);
                          GrB_Matrix_clear (tmp);
                     }
                     info = GrB_assign (Rt, GrB_NULL, GrB_NULL, tmp, node_idx, node_count, GrB_ALL, dim,
                                        GrB_NULL);
                     ASSERT(info == GrB_SUCCESS);
                     info = GrB_assign (Rt, GrB_NULL, GrB_NULL, tmp, GrB_ALL, dim, node_idx, node_count,
                                        GrB_DESC_T0);
                     ASSERT(info == GrB_SUCCESS);
                }
	}

        GrB_Matrix_clear (tmp);
        info = GxB_Matrix_resize (tmp, node_count, node_count);
        ASSERT(info == GrB_SUCCESS);
	/* Delete node labels. */
	int node_type_count = Graph_LabelTypeCount(g);
	for(int i = 0; i < node_type_count; i++) {
                GrB_Matrix L = Graph_GetLabelMatrix(g, i);
		GrB_Matrix_assign (L, GrB_NULL, GrB_NULL, tmp, node_idx, node_count, node_idx, node_count,
                                   GrB_NULL);
	}

	for(uint i = 0; i < node_count; i++) {
		Node *n = nodes + i;
		DataBlock_DeleteItem(g->nodes, ENTITY_GET_ID(n));
	}

	// Clean up.
        rm_free (node_idx);
        array_free (val);
	GrB_free (&tmp);
}

static void _BulkDeleteEdges(Graph *g, Edge *edges, size_t edge_count) {
	ASSERT(g && g->_writelocked && edges && edge_count > 0);

	int relationCount = Graph_RelationTypeCount(g);
	GrB_Matrix masks[relationCount];
	for(int i = 0; i < relationCount; i++) masks[i] = NULL;
	bool update_adj_matrices = false;

	bool maintain_transpose;
	Config_Option_get(Config_MAINTAIN_TRANSPOSE, &maintain_transpose);

	for(int i = 0; i < edge_count; i++) {
		Edge *e = edges + i;
		int r = Edge_GetRelationID(e);
		NodeID src_id = Edge_GetSrcNodeID(e);
		NodeID dest_id = Edge_GetDestNodeID(e);
		EdgeID edge_id;
		GrB_Matrix R = Graph_GetRelationMatrix(g, r);  // Relation matrix.
		GrB_Matrix TR = maintain_transpose ? Graph_GetTransposedRelationMatrix(g, r) : NULL;
		GrB_Matrix_extractElement(&edge_id, R, src_id, dest_id);

		if(SINGLE_EDGE(edge_id)) {
			update_adj_matrices = true;
			GrB_Matrix mask = masks[r];    // mask noteing all deleted edges.
			// Get mask of this relation type.
			if(mask == NULL) {
				GrB_Matrix_new(&mask, GrB_BOOL, Graph_RequiredMatrixDim(g), Graph_RequiredMatrixDim(g));
				masks[r] = mask;
			}
			// Update mask.
			GrB_Matrix_setElement_BOOL(mask, true, src_id, dest_id);
		} else {
			/* Multiple edges connecting src to dest
			 * locate specific edge and remove it
			 * revert back from array representation to edge ID
			 * incase we're left with a single edge connecting src to dest. */

			int i = 0;
			EdgeID id = ENTITY_GET_ID(e);
			EdgeID *multi_edges = (EdgeID *)edge_id;
			int multi_edge_count = array_len(multi_edges);

			// Locate edge within edge array.
			for(; i < multi_edge_count; i++) if(multi_edges[i] == id) break;
			ASSERT(i < multi_edge_count);

			/* Remove edge from edge array
			 * migrate last edge ID and reduce array size.
			 * TODO: reallocate array of size / capacity ratio is high. */
			multi_edges[i] = multi_edges[multi_edge_count - 1];
			array_pop(multi_edges);

			/* Incase we're left with a single edge connecting src to dest
			 * revert back from array to scalar. */
			if(array_len(multi_edges) == 1) {
				edge_id = multi_edges[0];
				array_free(multi_edges);
				GrB_Matrix_setElement(R, SET_MSB(edge_id), src_id, dest_id);
			}

			if(TR) {
				/* We must make the matching updates to the transposed matrix.
				 * First, extract the element that is known to be an edge array. */
				GrB_Matrix_extractElement(&edge_id, TR, dest_id, src_id);
				multi_edges = (EdgeID *)edge_id;
				int multi_edge_count = array_len(multi_edges);
				multi_edges[i] = multi_edges[multi_edge_count - 1];
				array_pop(multi_edges);
				// Free and replace the array if it now has 1 element.
				if(array_len(multi_edges) == 1) {
					edge_id = multi_edges[0];
					array_free(multi_edges);
					GrB_Matrix_setElement(TR, SET_MSB(edge_id), dest_id, src_id);
				}
			}
		}

		// Free and remove edges from datablock.
		DataBlock_DeleteItem(g->edges, ENTITY_GET_ID(e));
	}

	if(update_adj_matrices) {
		GrB_Matrix remaining_mask;
		GrB_Matrix_new(&remaining_mask, GrB_BOOL, Graph_RequiredMatrixDim(g), Graph_RequiredMatrixDim(g));
		GrB_Descriptor desc;    // GraphBLAS descriptor.
		GrB_Descriptor_new(&desc);
		// Descriptor sets to clear entry according to mask.
		GrB_Descriptor_set(desc, GrB_MASK, GrB_COMP);

		// Clear updated output matrix before assignment.
		GrB_Descriptor_set(desc, GrB_OUTP, GrB_REPLACE);

		bool maintain_transpose;
		Config_Option_get(Config_MAINTAIN_TRANSPOSE, &maintain_transpose);
		for(int r = 0; r < relationCount; r++) {
			GrB_Matrix mask = masks[r];
			GrB_Matrix R = Graph_GetRelationMatrix(g, r);  // Relation matrix.
			if(mask) {
				// Remove every entry of R marked by Mask.
				// Desc: GrB_MASK = GrB_COMP,  GrB_OUTP = GrB_REPLACE.
				// R = R & !mask.
				GrB_Matrix_apply(R, mask, GrB_NULL, GrB_IDENTITY_UINT64, R, desc);
				if(maintain_transpose) {
					GrB_Matrix tM = Graph_GetTransposedRelationMatrix(g, r);  // Transposed relation mapping matrix.
					// Transpose mask (this cannot be done by descriptor).
					GrB_transpose(mask, GrB_NULL, GrB_NULL, mask, GrB_NULL);
					// tM = tM & !mask.
					GrB_Matrix_apply(tM, mask, GrB_NULL, GrB_IDENTITY_UINT64, tM, desc);
				}
				GrB_free(&mask);
			}

			// Collect remaining edges. remaining_mask = remaining_mask + R.
			GrB_eWiseAdd_Matrix_Semiring(remaining_mask, GrB_NULL, GrB_NULL, GxB_ANY_PAIR_BOOL, remaining_mask,
										 R, GrB_NULL);
		}

		GrB_Matrix adj_matrix = Graph_GetAdjacencyMatrix(g);
		GrB_Matrix t_adj_matrix = Graph_GetTransposedAdjacencyMatrix(g);
		// To calculate edges to delete, remove all the remaining edges from "The" adjency matrix.
		// Set descriptor mask to default.
		GrB_Descriptor_set(desc, GrB_MASK, GxB_DEFAULT);
		// adj_matrix = adj_matrix & remaining_mask.
		GrB_Matrix_apply(adj_matrix, remaining_mask, GrB_NULL, GrB_IDENTITY_BOOL, adj_matrix, desc);
		// Transpose remaining_mask.
		GrB_transpose(remaining_mask, GrB_NULL,  GrB_NULL, remaining_mask, GrB_NULL);
		// t_adj_matrix = t_adj_matrix & remaining_mask.
		GrB_Matrix_apply(t_adj_matrix, remaining_mask, GrB_NULL, GrB_IDENTITY_BOOL, t_adj_matrix, desc);

		GrB_free(&remaining_mask);
		GrB_free(&desc);
	}
        GrB_wait ();

}

#define is_entity_lt(a, b) (ENTITY_GET_ID((a)) < ENTITY_GET_ID((b)))

/* Removes both nodes and edges from graph. */
void Graph_BulkDelete(Graph *g, Node *nodes, uint node_count, Edge *edges, uint edge_count,
					  uint *node_deleted, uint *edge_deleted) {
	ASSERT(g);

	*edge_deleted = 0;
	*node_deleted = 0;

	if(node_count) {
             _BulkDeleteNodes(g, nodes, node_count, node_deleted, edge_deleted);
        }

	if(edge_count) {
		// Filter out explicit edges which were removed by _BulkDeleteNodes.
		if(node_count) {
			for(int i = 0; i < edge_count; i++) {
				Edge *e = edges + i;
				NodeID src = Edge_GetSrcNodeID(e);
				NodeID dest = Edge_GetDestNodeID(e);

				if(!DataBlock_GetItem(g->nodes, src) || !DataBlock_GetItem(g->nodes, dest)) {
					/* Edge already removed due to node removal.
					* Replace current edge with last edge. */
					edges[i] = edges[edge_count - 1];

					// Update indices.
					i--;
					edge_count--;
				}
			}
		}

		/* it might be that edge_count dropped to 0
		 * due to implicit edge deletion. */
		if(edge_count == 0) return;

		// Removing duplicates.
		QSORT(Edge, edges, edge_count, is_entity_lt);

		size_t uniqueIdx = 0;
		for(int i = 0; i < edge_count; i++) {
			// As long as current is the same as follows.
			while(i < edge_count - 1 && ENTITY_GET_ID(edges + i) == ENTITY_GET_ID(edges + i + 1)) i++;

			if(uniqueIdx < i) edges[uniqueIdx] = edges[i];
			uniqueIdx++;
		}

		edge_count = uniqueIdx;
		_BulkDeleteEdges(g, edges, edge_count);
	}

	*edge_deleted += edge_count;
}

DataBlockIterator *Graph_ScanNodes(const Graph *g) {
	ASSERT(g);
	return DataBlock_Scan(g->nodes);
}

DataBlockIterator *Graph_ScanEdges(const Graph *g) {
	ASSERT(g);
	return DataBlock_Scan(g->edges);
}

int Graph_AddLabel(Graph *g) {
	ASSERT(g);
	RG_Matrix m = RG_Matrix_New(GrB_BOOL, Graph_RequiredMatrixDim(g), Graph_RequiredMatrixDim(g));
	array_append(g->labels, m);
	return array_len(g->labels) - 1;
}

int Graph_AddRelationType(Graph *g) {
	ASSERT(g);

	size_t dims = Graph_RequiredMatrixDim(g);
	RG_Matrix m = RG_Matrix_New(GrB_UINT64, dims, dims);
	g->relations = array_append(g->relations, m);
	bool maintain_transpose;
	Config_Option_get(Config_MAINTAIN_TRANSPOSE, &maintain_transpose);

	if(maintain_transpose) {
		RG_Matrix tm = RG_Matrix_New(GrB_UINT64, dims, dims);
		g->t_relations = array_append(g->t_relations, tm);
	}

	int relationID = Graph_RelationTypeCount(g) - 1;
	return relationID;
}

GrB_Matrix Graph_GetAdjacencyMatrix(const Graph *g) {
	ASSERT(g);
	RG_Matrix m = g->adjacency_matrix;
	g->SynchronizeMatrix(g, m);
	return RG_Matrix_Get_GrB_Matrix(m);
}

// Get the transposed adjacency matrix.
GrB_Matrix Graph_GetTransposedAdjacencyMatrix(const Graph *g) {
	ASSERT(g);
	RG_Matrix m = g->_t_adjacency_matrix;
	g->SynchronizeMatrix(g, m);
	return RG_Matrix_Get_GrB_Matrix(m);
}

GrB_Matrix Graph_GetLabelMatrix(const Graph *g, int label_idx) {
	ASSERT(g && label_idx < array_len(g->labels));
	RG_Matrix m = g->labels[label_idx];
	g->SynchronizeMatrix(g, m);
	return RG_Matrix_Get_GrB_Matrix(m);
}

GrB_Matrix Graph_GetRelationMatrix(const Graph *g, int relation_idx) {
	ASSERT(g && (relation_idx == GRAPH_NO_RELATION || relation_idx < Graph_RelationTypeCount(g)));

	if(relation_idx == GRAPH_NO_RELATION) {
		return Graph_GetAdjacencyMatrix(g);
	} else {
		RG_Matrix m = g->relations[relation_idx];
		g->SynchronizeMatrix(g, m);
		return RG_Matrix_Get_GrB_Matrix(m);
	}
}

GrB_Matrix Graph_GetTransposedRelationMatrix(const Graph *g, int relation_idx) {
	ASSERT(g && (relation_idx == GRAPH_NO_RELATION || relation_idx < Graph_RelationTypeCount(g)));

	if(relation_idx == GRAPH_NO_RELATION) {
		return Graph_GetTransposedAdjacencyMatrix(g);
	} else {
		ASSERT(g->t_relations && "tried to retrieve nonexistent transposed matrix.");

		RG_Matrix m = g->t_relations[relation_idx];
		g->SynchronizeMatrix(g, m);
		return RG_Matrix_Get_GrB_Matrix(m);
	}
}

GrB_Matrix Graph_GetZeroMatrix(const Graph *g) {
	GrB_Index nvals;
	RG_Matrix z = g->_zero_matrix;
	g->SynchronizeMatrix(g, z);

	// Make sure zero matrix is indeed empty.
	GrB_Matrix grb_z = RG_Matrix_Get_GrB_Matrix(z);
	GrB_Matrix_nvals(&nvals, grb_z);
	ASSERT(nvals == 0);
	return grb_z;
}

void Graph_Free(Graph *g) {
	ASSERT(g);
	// Free matrices.
	Entity *en;
	DataBlockIterator *it;
	RG_Matrix_Free(g->_zero_matrix);
	RG_Matrix_Free(g->adjacency_matrix);
	RG_Matrix_Free(g->_t_adjacency_matrix);

	_Graph_FreeRelationMatrices(g);
	array_free(g->relations);
	array_free(g->t_relations);

	uint32_t labelCount = array_len(g->labels);
	for(int i = 0; i < labelCount; i++) {
		RG_Matrix_Free(g->labels[i]);
	}
	array_free(g->labels);

	it = Graph_ScanNodes(g);
	while((en = (Entity *)DataBlockIterator_Next(it, NULL)) != NULL)
		FreeEntity(en);

	DataBlockIterator_Free(it);

	it = Graph_ScanEdges(g);
	while((en = DataBlockIterator_Next(it, NULL)) != NULL)
		FreeEntity(en);

	DataBlockIterator_Free(it);

	// Free blocks.
	DataBlock_Free(g->nodes);
	DataBlock_Free(g->edges);

	int res;
	UNUSED(res);
	res = pthread_mutex_destroy(&g->_writers_mutex);
	ASSERT(res == 0);

	if(g->_writelocked) Graph_ReleaseLock(g);
	res = pthread_rwlock_destroy(&g->_rwlock);
	ASSERT(res == 0);

	rm_free(g);
}

