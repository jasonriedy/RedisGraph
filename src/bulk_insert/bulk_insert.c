/*
* Copyright 2018-2020 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "bulk_insert.h"
#include "../config.h"
#include "../util/arr.h"
#include "../schema/schema.h"
#include "../util/rmalloc.h"
#include "../util/datablock/datablock.h"
#include <errno.h>
#include <assert.h>

// The first byte of each property in the binary stream
// is used to indicate the type of the subsequent SIValue
typedef enum {
	BI_NULL,
	BI_BOOL,
	BI_DOUBLE,
	BI_STRING,
	BI_LONG
} TYPE;

// Read the header of a data stream to parse its property keys and update schemas.
static Attribute_ID *_BulkInsert_ReadHeader(GraphContext *gc, SchemaType t,
											const char *data, size_t *data_idx,
											int *label_id, unsigned int *prop_count) {
	/* Binary header format:
	 * - entity name : null-terminated C string
	 * - property count : 4-byte unsigned integer
	 * [0..property_count] : null-terminated C string
	 */
	// First sequence is entity name
	const char *name = data + *data_idx;
	*data_idx += strlen(name) + 1;
	Schema *schema = GraphContext_GetSchema(gc, name, t);
	if(schema == NULL) schema = GraphContext_AddSchema(gc, name, t);
	*label_id = schema->id;

	// Next 4 bytes are property count
	*prop_count = *(unsigned int *)&data[*data_idx];
	*data_idx += sizeof(unsigned int);

	if(*prop_count == 0) return NULL;
	Attribute_ID *prop_indicies = malloc(*prop_count * sizeof(Attribute_ID));

	// The rest of the line is [char *prop_key] * prop_count
	for(unsigned int j = 0; j < *prop_count; j ++) {
		char *prop_key = (char *)data + *data_idx;
		*data_idx += strlen(prop_key) + 1;

		// Add properties to schemas
		prop_indicies[j] = GraphContext_FindOrAddAttribute(gc, prop_key);
	}

	return prop_indicies;
}

// Read an SIValue from the data stream and update the index appropriately
static inline SIValue _BulkInsert_ReadProperty(const char *data, size_t *data_idx) {
	/* Binary property format:
	 * - property type : 1-byte integer corresponding to TYPE enum
	 * - Nothing if type is NULL
	 * - 1-byte true/false if type is boolean
	 * - 8-byte double if type is numeric
	 * - Null-terminated C string if type is string
	 */
	SIValue v;
	TYPE t = data[*data_idx];
	*data_idx += 1;
	if(t == BI_NULL) {
		v = SI_NullVal();
	} else if(t == BI_BOOL) {
		bool b = data[*data_idx];
		*data_idx += 1;
		v = SI_BoolVal(b);
	} else if(t == BI_DOUBLE) {
		double d = *(double *)&data[*data_idx];
		*data_idx += sizeof(double);
		v = SI_DoubleVal(d);
	} else if(t == BI_LONG) {
		int64_t d = *(int64_t *)&data[*data_idx];
		*data_idx += sizeof(int64_t);
		v = SI_LongVal(d);
	} else if(t == BI_STRING) {
		const char *s = data + *data_idx;
		*data_idx += strlen(s) + 1;
		// The string itself will be cloned when added to the GraphEntity properties.
		v = SI_ConstStringVal((char *)s);
	} else {
		assert(0);
	}
	return v;
}

int _BulkInsert_ProcessNodeFile(RedisModuleCtx *ctx, GraphContext *gc, 
                                const char *data,
                                size_t data_len) {
	size_t data_idx = 0;

	int label_id;
	unsigned int prop_count;
	Attribute_ID *prop_indicies = _BulkInsert_ReadHeader(gc, SCHEMA_NODE, data, &data_idx, &label_id,
														 &prop_count);
        Graph *g = gc->g;
        DataBlock *nodes = g->nodes;

        /* All nodes are pre-allocated and matrices extended in
           bulk_insert().  nodes->itemCount has *not* been updated.
           
           The number of nodes in this "file" is not known, so this
           routine makes two passes.  The first one adds properties
           and counts.

           If there is a label, the second pass extracts the ids to
           update the label matrices.

           This routine does *not* back-fill deleted node indices.
           The allocated ids then begin at nodes->itemCount and only
           increase.
        */

        const uint64_t first_id = nodes->itemCount;
        size_t num_ids = 0;

        while (data_idx < data_len) {
             // Cheat and inline datablock allocation.
             const uint64_t id = nodes->itemCount++;
             ++num_ids;
             
             DataBlockItemHeader *item_header = DataBlock_GetItemHeader(nodes, id);
             MARK_HEADER_AS_NOT_DELETED(item_header);

             Entity *en = (Entity*)ITEM_DATA(item_header);
             en->prop_count = 0;
             en->properties = NULL;

             for(unsigned int i = 0; i < prop_count; i++) {
                  SIValue value = _BulkInsert_ReadProperty(data, &data_idx);
                  // Cypher does not support NULL as a property value.
                  // If we encounter one here, simply skip it.
                  if(SI_TYPE(value) == T_NULL) continue;
                  Node n = { .entity = en, .id = id};
                  GraphEntity_AddProperty((GraphEntity *)&n, prop_indicies[i], value);
             }
        }

        if (label_id != GRAPH_NO_LABEL) {
             GrB_Index *I = rm_malloc (num_ids * sizeof(*I));
             assert (I != NULL);
             bool *X = rm_malloc (num_ids * sizeof(*X));
             assert (X != NULL);
             if (I == NULL || X == NULL) {
                  free(prop_indicies);
                  free (I);
                  free (X);
                  return BULK_FAIL;  // Assume the nodes are rolled back somehow.
             }

             for (size_t k = 0; k < num_ids; ++k) I[k] = first_id + k;

             GrB_Matrix m = Graph_GetLabelMatrix(g, label_id);
             GrB_Matrix tmp;
             GrB_Index nr;
             GrB_Info info;
             info = GrB_Matrix_nrows (&nr, m);
             assert (info == GrB_SUCCESS); // Really should roll back the insertion.
             if (nr < first_id + num_ids) {
                  nr = first_id + num_ids;
                  GxB_Matrix_resize (m, nr, nr);
             }

             info = GrB_Matrix_new (&tmp, GrB_BOOL, num_ids, num_ids);
             assert (info == GrB_SUCCESS);

             for (size_t k = 0; k < num_ids; ++k) X[k] = true;
             info = GrB_Matrix_build (tmp, I, I, X, num_ids, GxB_PAIR_BOOL);
             assert (info == GrB_SUCCESS);

             // Adjust to the first_id offset into m.
             for (size_t k = 0; k < num_ids; ++k) I[k] += first_id;
             info = GrB_assign (m, GrB_NULL, GrB_NULL, tmp, I, num_ids, I, num_ids, GrB_NULL);
             assert (info == GrB_SUCCESS);

             GrB_free (&tmp);
             rm_free (X);
             rm_free (I);
        }

	free(prop_indicies);
	return BULK_OK;
}

// Modified from src/graph/graph.c
static void add_relation_id (EdgeID* z_out, EdgeID x_in, EdgeID y_newid)
{
	EdgeID *ids;
	/* Single edge ID,
	 * switching from single edge ID to multiple IDs. */
	if (SINGLE_EDGE(x_in) && SINGLE_EDGE(y_newid)) {
		ids = array_new(EdgeID, 2);
		ids = array_append(ids, SINGLE_EDGE_ID(x_in));
		ids = array_append(ids, SINGLE_EDGE_ID(y_newid));
		// TODO: Make sure MSB of ids isn't on.
		*z_out = (EdgeID)ids;
	} else if ((!(SINGLE_EDGE(x_in)) && SINGLE_EDGE(y_newid))) {
		// Multiple edges, adding another edge.
		ids = (EdgeID *)(x_in);
		ids = array_append(ids, SINGLE_EDGE_ID(y_newid));
		*z_out = (EdgeID)ids;
	} else if ((SINGLE_EDGE(x_in) && !(SINGLE_EDGE(y_newid)))) {
		// Multiple edges, adding another edge.
		ids = (EdgeID *)(y_newid);
		ids = array_append(ids, SINGLE_EDGE_ID(x_in));
		*z_out = (EdgeID)ids;
	} else {
             ids = (EdgeID*)x_in;
             array_ensure_append (ids, (EdgeID*)y_newid, array_len((EdgeID*)y_newid), EdgeID);
             *z_out = (EdgeID)ids;
             // y_newid should be freed.
	}
}

int _BulkInsert_ProcessRelationFile(RedisModuleCtx *ctx, GraphContext *gc,
                                    GrB_Index *I, GrB_Index *J,
                                    const char *data, size_t data_len) {
     if (!data_len) return BULK_OK; // Paranoia check.
     size_t data_idx = 0;

        Graph *g = gc->g;
	int reltype_id;
	unsigned int prop_count;
	// Read property keys from header and update schema
	Attribute_ID *prop_indicies = _BulkInsert_ReadHeader(gc, SCHEMA_EDGE, data, &data_idx, &reltype_id,
                                                             &prop_count);
	NodeID src;
	NodeID dest;
        size_t num_ids = 0;
        size_t max_node_id = 0;

        size_t relations_in_chunk = 0;

        // The first pass counts and reads all the indices.
	while(data_idx < data_len) {
             I[relations_in_chunk] = *(NodeID *)&data[data_idx];
             data_idx += sizeof(NodeID);
             J[relations_in_chunk] = *(NodeID *)&data[data_idx];
             data_idx += sizeof(NodeID);
             ++relations_in_chunk;

             // Must parse the variable-length properties to move data_idx forward.
             for(unsigned int i = 0; i < prop_count; i ++)
                  _BulkInsert_ReadProperty(data, &data_idx);
        }

        // Pre-allocate the edge records.
        const EdgeID first_id = g->edges->itemCount;
        for (size_t k = 0; k < relations_in_chunk; ++k) {
             EdgeID id;
             Entity *en = DataBlock_AllocateItem(g->edges, &id);
             en->prop_count = 0;
             en->properties = NULL;
        }

        // Now add properties.
        if (prop_count) {
             SIValue *tmp_val = rm_malloc (prop_count * sizeof(*tmp_val));
             Attribute_ID *tmp_id = rm_malloc (prop_count * sizeof(*tmp_id));
             data_idx = 0;
             for (size_t k = 0; k < relations_in_chunk; ++k) {
                  size_t nprop = 0;
                  data_idx += 16; // Skip vertices;
                  for(unsigned int i = 0; i < prop_count; i ++) {
                       tmp_val[nprop] = _BulkInsert_ReadProperty(data, &data_idx);
                       // Cypher does not support NULL as a property value.
                       // If we encounter one here, simply skip it.
                       if(SI_TYPE(tmp_val[nprop]) == T_NULL) continue;
                       tmp_id[nprop] = prop_indicies[i];
                       ++nprop;
                  }
                  if (nprop) {
                       EntityProperty *prop = rm_malloc (nprop * sizeof(*prop));
                       for (size_t prop_idx = 0; prop_idx < nprop; ++prop_idx) {
                            prop[prop_idx].id = prop_idx;
                            prop[prop_idx].value = SI_CloneValue(tmp_val[prop_idx]);
                            SIValue_Free (tmp_val[prop_idx]);
                       }
                       ((Entity*)DataBlock_GetItem(g->edges, first_id+k))->properties = prop;
                       ((Entity*)DataBlock_GetItem(g->edges, first_id+k))->prop_count = nprop;
                  }
             }
             rm_free (tmp_id);
             rm_free (tmp_val);
        }
        free (prop_indicies);
        prop_indicies = NULL;

        /* Create and union in the adjacency matrices.  Matrices
           already have been resized.  Now build for the full size and eWiseAdd. */
	//const GrB_Index dims = Graph_RequiredMatrixDim(g);
        GrB_Matrix addl_adj;
        GrB_Index nrows;

        {
             GrB_Matrix adj = Graph_GetAdjacencyMatrix (g);
             GrB_Matrix tadj = Graph_GetTransposedAdjacencyMatrix (g);
             GrB_Info info;

             info = GrB_Matrix_nrows (&nrows, adj);
             assert (info == GrB_SUCCESS);

             bool *X = rm_malloc (relations_in_chunk * sizeof (*X));
             assert (X != NULL);
             for (GrB_Index k = 0; k < relations_in_chunk; ++k) X[k] = true;
        
             info = GrB_Matrix_new (&addl_adj, GrB_BOOL, nrows, nrows);
             info = GrB_Matrix_build (addl_adj, I, J, X, relations_in_chunk, GxB_PAIR_BOOL);
             assert (info == GrB_SUCCESS);

             info = GrB_eWiseAdd (adj, GrB_NULL, GrB_NULL, GrB_LOR, adj, addl_adj, GrB_NULL);
             assert (info == GrB_SUCCESS);
             info = GrB_eWiseAdd (tadj, GrB_NULL, GrB_NULL, GrB_LOR, tadj, addl_adj, GrB_DESC_T1);
             assert (info == GrB_SUCCESS);

             rm_free (X);
        }

        // Find duplicate edges for the relation matrix, allocated via the header reader
        /* Note: If a GraphBLAS implementation library properly
          supports the dup GrB_BinaryOp parameter in GrB_Matrix_build,
          *AND* if the GraphBLAS routines can manipulate both the host
          memory and the GraphBLAS memory, that operation could extend
          the id arrays itself.  Managing memory out-of-band that way
          is a bit tricky if the build routine is parallel.  That
          operation also would replace extracting existing relations
          and re-adding them. 

          The current implementation does not assume that the host and
          GraphBLAS can manage each others' memory, e.g. GraphBLAS is
          on an accelerator. */

        {
             /* Build a hash table of the current edges -> [edge_id]. */
             rax *rt = raxNew();
             EdgeID *all_ids = rm_malloc (relations_in_chunk * sizeof (*all_ids));
             for (size_t k = 0; k < relations_in_chunk; ++k) all_ids[k] = SET_MSB(first_id + k);
             
             for (size_t k = 0; k < relations_in_chunk; ++k) {
                  int lookup;
                  EdgeID cur_id = all_ids[k];
                  EdgeID *old_id_ptr;
                  GrB_Index IJ[2] = { I[k], J[k] };

                  lookup = raxTryInsert (rt, (unsigned char*)IJ, sizeof(IJ), &all_ids[k], (void**)&old_id_ptr);
                  if (lookup == 0) {
                       add_relation_id (old_id_ptr, *old_id_ptr, cur_id);
                       if (!(SINGLE_EDGE(cur_id))) array_free ((EdgeID*)cur_id);
                       all_ids[k] = -1; // Mark as a duplicate.
                  }
             }
             
             /* Get the existing relation matrix and ensure it's the correct
                size.  The latter may be redundant. */
             GrB_Matrix relmat = Graph_GetRelationMatrix (g, reltype_id);
             GrB_Matrix relmat_slice;
             GrB_Info info;

             info = GxB_Matrix_resize (relmat, nrows, nrows);
             assert (info == GrB_SUCCESS);
             info = GrB_Matrix_new (&relmat_slice, GrB_UINT64, nrows, nrows);
             assert (info == GrB_SUCCESS);
             info = GrB_Matrix_extract (relmat_slice, addl_adj, GrB_NULL, relmat,
                                        GrB_ALL, nrows, GrB_ALL, nrows, GrB_NULL);
             // Possible alternative:
             /* info = GrB_Matrix_eWiseMult (relmat_slice, GrB_NULL, GrB_NULL, GrB_SECOND_UINT64,  */
             /*                              addl_adj, relmat, GrB_NULL); */
             assert (info == GrB_SUCCESS);

             GrB_Index old_relmat_nvals;
             info = GrB_Matrix_nvals (&old_relmat_nvals, relmat_slice);
             assert (info == GrB_SUCCESS);
             if (old_relmat_nvals) {
                  // Extract and insert into the hash table.
                  GrB_Index *old_I;
                  GrB_Index *old_J;
                  uint64_t *old_X;
                  GrB_Index actually_extracted;
                  
                  old_I = rm_malloc (2 * old_relmat_nvals * sizeof (*old_I));
                  old_J = &old_I[old_relmat_nvals];
                  old_X = rm_malloc (old_relmat_nvals * sizeof (*old_X));
                  info = GrB_Matrix_extractTuples (old_I, old_J, old_X, &actually_extracted, relmat_slice);
                  assert (info == GrB_SUCCESS);
                  assert (actually_extracted == old_relmat_nvals);

                  for (size_t k = 0; k < old_relmat_nvals; ++k) {
                       int lookup;
                       EdgeID old_id = old_X[k];
                       EdgeID *new_id_ptr;
                       GrB_Index IJ[2] = { old_I[k], old_J[k] };

                       new_id_ptr = raxFind (rt, (unsigned char*)IJ, sizeof(IJ));
                       assert (new_id_ptr != raxNotFound); // Extracted through the IJ mask.
                       add_relation_id (new_id_ptr, *new_id_ptr, old_id);
                       if (!(SINGLE_EDGE(old_id))) array_free ((EdgeID*)old_id);
                  }
                  rm_free (old_X);
                  rm_free (old_I);
             }
             // No longer need the hash table, and it will be incorrect shortly.
             raxFree (rt);
             rt = NULL;

             // Build the matrix to be inserted.
             info = GrB_Matrix_clear (relmat_slice);
             assert (info == GrB_SUCCESS);

             // Compress I, J, all_ids
             size_t first = 0, new_nrels = 0;
             assert (all_ids[0] != -1); // Don't feel like dealing with it.
             while (++first != relations_in_chunk) {
                  if (all_ids[first] == -1) {
                       I[new_nrels] = I[first];
                       J[new_nrels] = J[first];
                       all_ids[new_nrels] = all_ids[first];
                       ++new_nrels;
                  }
             }
             assert (new_nrels > 0); // Utter paranoia.

             info = GrB_Matrix_build (relmat_slice, I, J, all_ids, new_nrels, GxB_ANY_UINT64);
             assert (info == GrB_SUCCESS);

             // At last, replace the entries.  These could be GrB_assign through an addl_adj mask.
             info = GrB_eWiseAdd (relmat, GrB_NULL, GrB_NULL, GrB_SECOND_UINT64, relmat, relmat_slice, GrB_NULL);
             if (Config_MaintainTranspose()) { // XXX: Creation, so assume symmetric...
                  GrB_Matrix t_relmat = Graph_GetTransposedRelationMatrix (g, reltype_id);
                  info = GrB_eWiseAdd (relmat, GrB_NULL, GrB_NULL, GrB_SECOND_UINT64, relmat, relmat_slice, GrB_DESC_T1);
             }

             GrB_free (&relmat_slice);
             GrB_free (&addl_adj);
             rm_free (all_ids);
        }
        return BULK_OK;
}

int _BulkInsert_InsertNodes(RedisModuleCtx *ctx, GraphContext *gc,
                            int token_count,
                            RedisModuleString ***argv, int *argc) {
	int rc;
	for(int i = 0; i < token_count; i ++) {
		size_t len;
		// Retrieve a pointer to the next binary stream and record its length
		const char *data = RedisModule_StringPtrLen(**argv, &len);
		*argv += 1;
		*argc -= 1;
		rc = _BulkInsert_ProcessNodeFile(ctx, gc, data, len);
		assert(rc == BULK_OK);
	}
	return BULK_OK;
}

int _BulkInsert_Insert_Edges(RedisModuleCtx *ctx, GraphContext *gc,
                             GrB_Index *I, GrB_Index *J, 
                             int token_count, RedisModuleString ***argv, int *argc) {
	int rc;
	for(int i = 0; i < token_count; i ++) {
		size_t len;
		// Retrieve a pointer to the next binary stream and record its length
		const char *data = RedisModule_StringPtrLen(**argv, &len);
		*argv += 1;
		*argc -= 1;
		rc = _BulkInsert_ProcessRelationFile(ctx, gc, I, J, data, len);
		assert(rc == BULK_OK);
	}
	return BULK_OK;
}

int BulkInsert(RedisModuleCtx *ctx, GraphContext *gc, 
               long long nodes_in_query, long long relations_in_query, 
               RedisModuleString **argv, int argc) {
        int retval = BULK_FAIL;
        GrB_Index *I = NULL, *J = NULL;
	if(argc < 2) {
		RedisModule_ReplyWithError(ctx, "Bulk insert format error, failed to parse bulk insert sections.");
		return BULK_FAIL;
	}

	// Disable matrix synchronization for bulk insert operation
        MATRIX_POLICY prev_policy = Graph_GetMatrixPolicy (gc->g);
	Graph_SetMatrixPolicy(gc->g, RESIZE_TO_CAPACITY);

	// Read the number of node tokens
	long long node_token_count;
	long long relation_token_count;
	if(RedisModule_StringToLongLong(*argv++, &node_token_count)  != REDISMODULE_OK) {
		RedisModule_ReplyWithError(ctx, "Error parsing number of node descriptor tokens.");
		retval = BULK_FAIL;
                goto done;
	}

	if(RedisModule_StringToLongLong(*argv++, &relation_token_count)  != REDISMODULE_OK) {
		RedisModule_ReplyWithError(ctx, "Error parsing number of relation descriptor tokens.");
		retval = BULK_FAIL;
                goto done;
	}
	argc -= 2;

        size_t num_orig_nodes = Graph_NodeCount(gc->g);
	if(node_token_count > 0) {
             // Allocate or extend datablocks and matrices to accommodate all incoming entities
             Graph_AllocateNodes(gc->g, nodes_in_query + num_orig_nodes);
             retval = _BulkInsert_InsertNodes(ctx, gc, node_token_count, &argv, &argc);
             if(retval != BULK_OK) goto done;
	}

	if(relation_token_count > 0) {
             // No simple way to query the number of edges...
             DataBlock_AccommodateAdditional (gc->g->edges, relations_in_query);
             // Overkill.
             I = rm_malloc (relations_in_query * sizeof (*I));
             J = rm_malloc (relations_in_query * sizeof (*J));
             retval = _BulkInsert_Insert_Edges(ctx, gc, I, J, relation_token_count, &argv, &argc);
             if(retval != BULK_OK) goto done;
	}

	assert(argc == 0);

	retval = BULK_OK;

done:
        Graph_SetMatrixPolicy(gc->g, prev_policy);
        if (I != NULL) rm_free (I);
        if (J != NULL) rm_free (J);
        return retval;
}

