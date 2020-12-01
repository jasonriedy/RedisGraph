/*
* Copyright 2018-2020 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "bulk_insert.h"
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
                                long long nodes_in_query,
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
                  GraphEntity_AddProperty((GraphEntity *)en, prop_indicies[i], value);
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

             for (size_t k = 0; k < num_ids; ++k) I[k] = k;

             GrB_Matrix m = g->labels[label_id]->grb_matrix;
             GrB_Matrix tmp;
             GrB_Index nr;
             GrB_Info info;
             info = GrB_Matrix_nrows(&nr, m);
             assert (info == GrB_SUCCESS); // Really should roll back the insertion.
             assert (nr == first_id + num_ids);

             info = GrB_Matrix_new (&tmp, GrB_BOOL, nr, nr);
             assert (info != GrB_SUCCESS);

             for (size_t k = 0; k < num_ids; ++k) X[k] = true;
             info = GrB_Matrix_build (tmp, I, I, X, num_ids, GrB_NULL);
             assert (info != GrB_SUCCESS);

             // Adjust to the first_id offset into m.
             for (size_t k = 0; k < num_ids; ++k) I[k] += first_id;
             info = GxB_subassign (m, GrB_NULL, GrB_NULL, tmp, I, num_ids, I, num_ids, GrB_NULL);
             assert (info != GrB_SUCCESS);

             GrB_free (&tmp);
             rm_free (X);
             rm_free (I);
        }

	free(prop_indicies);
	return BULK_OK;
}

int _BulkInsert_ProcessRelationFile(RedisModuleCtx *ctx, GraphContext *gc, const size_t num_orig_nodes,
                                    GrB_Index *I, GrB_Index *J,
                                    const char *data, size_t data_len) {
	size_t data_idx = 0;

        /*
          There is a single relation_id for the batch.  Assume unique across all batches.

          The matrices already have been resized.

          Assume the new region is empty for the relation's matrices.  There may be multi-edges here.

          Not necessarily true for the adjacency matrices.  Those must be built and eWideAdd-ed.  Or just OR-in the relation matrix built here.
         */

        /* XXX:
           first pass is to allocate records and add properties
           second pass extracts the ids
           then extend the appropriate matrices

           accumulate the IJ
           extract what exists for the reltype_id
           update/allocate entries in that subgraph
           replace
         */
	int reltype_id;
	unsigned int prop_count;
	// Read property keys from header and update schema
	Attribute_ID *prop_indicies = _BulkInsert_ReadHeader(gc, SCHEMA_EDGE, data, &data_idx, &reltype_id,
														 &prop_count);
	NodeID src;
	NodeID dest;
        size_t num_ids = 0;
        size_t max_node_id = 0;

	while(data_idx < data_len) {
		Edge e;
		// Next 8 bytes are source ID
		src = *(NodeID *)&data[data_idx];
		data_idx += sizeof(NodeID);
		// Next 8 bytes are destination ID
		dest = *(NodeID *)&data[data_idx];
		data_idx += sizeof(NodeID);

                // Allocate the edge record, assuming src and dest are unique for this reltype_id


                // Process and add relation properties to this edge's record
		Graph_ConnectNodes(gc->g, src, dest, reltype_id, &e);

		if(prop_count == 0) continue;

		// Process and add relation properties
		for(unsigned int i = 0; i < prop_count; i ++) {
			SIValue value = _BulkInsert_ReadProperty(data, &data_idx);
			// Cypher does not support NULL as a property value.
			// If we encounter one here, simply skip it.
			if(SI_TYPE(value) == T_NULL) continue;
			GraphEntity_AddProperty((GraphEntity *)&e, prop_indicies[i], value);
		}
	}

        /*
          Ensure the relation matrices are large enough (and sanity check the adj matrices).
          Build a mask.
          Fetch the slice.
          For all in the mask, add to their data
          For all not in the mask, allocate their data
         */

done_with_relations:
	free(prop_indicies);
	return BULK_OK;
}

int _BulkInsert_InsertNodes(RedisModuleCtx *ctx, GraphContext *gc, long long nodes_in_query,
                            int token_count,
                            RedisModuleString ***argv, int *argc) {
	int rc;
	for(int i = 0; i < token_count; i ++) {
		size_t len;
		// Retrieve a pointer to the next binary stream and record its length
		const char *data = RedisModule_StringPtrLen(**argv, &len);
		*argv += 1;
		*argc -= 1;
		rc = _BulkInsert_ProcessNodeFile(ctx, gc, nodes_in_query, data, len);
		assert(rc == BULK_OK);
	}
	return BULK_OK;
}

int _BulkInsert_Insert_Edges(RedisModuleCtx *ctx, GraphContext *gc, const size_t num_orig_nodes,
                             GrB_Index *I, GrB_Index *J, 
                             int token_count, RedisModuleString ***argv, int *argc) {
	int rc;
	for(int i = 0; i < token_count; i ++) {
		size_t len;
		// Retrieve a pointer to the next binary stream and record its length
		const char *data = RedisModule_StringPtrLen(**argv, &len);
		*argv += 1;
		*argc -= 1;
		rc = _BulkInsert_ProcessRelationFile(ctx, gc, num_orig_nodes, I, J, data, len);
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

        size_t num_orig_nodes = gc->g->nodes->itemCount;
	if(node_token_count > 0) {
             long long initial_node_count = Graph_NodeCount(gc->g);
             // Allocate or extend datablocks and matrices to accommodate all incoming entities
             Graph_AllocateNodes(gc->g, nodes_in_query + initial_node_count);
             retval = _BulkInsert_InsertNodes(ctx, gc, nodes_in_query, node_token_count, &argv, &argc);
             if(retval != BULK_OK) goto done;
	}

	if(relation_token_count > 0) {
             DataBlock_AccommodateAdditional (gc->g->edges, relations_in_query);
             I = rm_malloc (relations_in_query * sizeof (*I));
             J = rm_malloc (relations_in_query * sizeof (*J));
             retval = _BulkInsert_Insert_Edges(ctx, gc, num_orig_nodes, I, J, relation_token_count, &argv, &argc);
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

