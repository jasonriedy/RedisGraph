/*
* Copyright 2018-2020 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "RG.h"
#include "reduce_count.h"
#include "../ops/ops.h"
#include "../../util/arr.h"
#include "../../query_ctx.h"
#include "../../arithmetic/aggregate_funcs/agg_funcs.h"
#include "../execution_plan_build/execution_plan_modify.h"

#if defined(_OPENMP)
#define OMP_(x)
#define OMP(x) OMP_(omp #x)
#else
#define OMP(x)
#endif

static int _identifyResultAndAggregateOps(OpBase *root, OpResult **opResult,
										  OpAggregate **opAggregate) {
	OpBase *op = root;
	// Op Results.
	if(op->type != OPType_RESULTS || op->childCount != 1) return 0;

	*opResult = (OpResult *)op;
	op = op->children[0];

	// Op Aggregate.
	if(op->type != OPType_AGGREGATE || op->childCount != 1) return 0;

	// Expecting a single aggregation, without ordering.
	*opAggregate = (OpAggregate *)op;
	if((*opAggregate)->aggregate_count != 1 || (*opAggregate)->key_count != 0) return 0;

	AR_ExpNode *exp = (*opAggregate)->aggregate_exps[0];

	// Make sure aggregation performs counting.
	if(exp->type != AR_EXP_OP ||
	   exp->op.f->aggregate != true ||
	   strcasecmp(exp->op.func_name, "count") ||
	   Aggregate_PerformsDistinct(exp->op.f->privdata)) return 0;

	// Make sure Count acts on an alias.
	if(exp->op.child_count != 1) return 0;

	AR_ExpNode *arg = exp->op.children[0];
	return (arg->type == AR_EXP_OPERAND &&
			arg->operand.type == AR_EXP_VARIADIC);
}

/* Checks if execution plan solely performs node count */
static int _identifyNodeCountPattern(OpBase *root, OpResult **opResult, OpAggregate **opAggregate,
									 OpBase **opScan, const char **label) {
	// Reset.
	*label = NULL;
	*opScan = NULL;
	*opResult = NULL;
	*opAggregate = NULL;

	if(!_identifyResultAndAggregateOps(root, opResult, opAggregate)) return 0;
	OpBase *op = ((OpBase *)*opAggregate)->children[0];

	// Scan, either a full node or label scan.
	if((op->type != OPType_ALL_NODE_SCAN &&
		op->type != OPType_NODE_BY_LABEL_SCAN) ||
	   op->childCount != 0) {
		return 0;
	}

	*opScan = op;
	if(op->type == OPType_NODE_BY_LABEL_SCAN) {
		NodeByLabelScan *labelScan = (NodeByLabelScan *)op;
		*label = labelScan->n.label;
	}

	return 1;
}

bool _reduceNodeCount(ExecutionPlan *plan) {
	/* We'll only modify execution plan if it is structured as follows:
	 * "Scan -> Aggregate -> Results" */
	const char *label;
	OpBase *opScan;
	OpResult *opResult;
	OpAggregate *opAggregate;

	/* See if execution-plan matches the pattern:
	 * "Scan -> Aggregate -> Results".
	 * if that's not the case, simply return without making any modifications. */
	if(!_identifyNodeCountPattern(plan->root, &opResult, &opAggregate, &opScan, &label)) return false;

	/* User is trying to get total number of nodes in the graph
	 * optimize by skiping SCAN and AGGREGATE. */
	SIValue nodeCount;
	GraphContext *gc = QueryCtx_GetGraphCtx();

	// If label is specified, count only labeled entities.
	if(label) {
		Schema *s = GraphContext_GetSchema(gc, label, SCHEMA_NODE);
		if(s) nodeCount = SI_LongVal(Graph_LabeledNodeCount(gc->g, s->id));
		else nodeCount = SI_LongVal(0); // Specified Label doesn't exists.
	} else {
		nodeCount = SI_LongVal(Graph_NodeCount(gc->g));
	}

	// Construct a constant expression, used by a new projection operation
	AR_ExpNode *exp = AR_EXP_NewConstOperandNode(nodeCount);
	// The new expression must be aliased to populate the Record.
	exp->resolved_name = opAggregate->aggregate_exps[0]->resolved_name;
	AR_ExpNode **exps = array_new(AR_ExpNode *, 1);
	exps = array_append(exps, exp);

	OpBase *opProject = NewProjectOp(opAggregate->op.plan, exps);

	// New execution plan: "Project -> Results"
	ExecutionPlan_RemoveOp(plan, opScan);
	OpBase_Free(opScan);

	ExecutionPlan_RemoveOp(plan, (OpBase *)opAggregate);
	OpBase_Free((OpBase *)opAggregate);

	ExecutionPlan_AddOp((OpBase *)opResult, opProject);
	return true;
}

/* Checks if execution plan solely performs edge count */
static bool _identifyEdgeCountPattern(OpBase *root, OpResult **opResult, OpAggregate **opAggregate,
									 OpBase **opTraverse, OpBase **opScan) {
	// Reset.
	*opScan = NULL;
	*opTraverse = NULL;
	*opResult = NULL;
	*opAggregate = NULL;

	if(!_identifyResultAndAggregateOps(root, opResult, opAggregate)) return false;
	OpBase *op = ((OpBase *)*opAggregate)->children[0];

	if(op->type != OPType_CONDITIONAL_TRAVERSE || op->childCount != 1) return false;
	*opTraverse = op;
	op = op->children[0];

	// Only a full node scan can be converted, as a labeled source acts as a filter
	// that may invalidate some of the edges.
	if(op->type != OPType_ALL_NODE_SCAN || op->childCount != 0) return false;
	*opScan = op;

	return true;
}

uint64_t _countRelationshipEdges(GrB_Matrix M) {
	/* TODO: to avoid this entire process keep track if
	 * M contains multiple edges between two given nodes
	 * if there are no multiple edges,
	 * i.e. `a` is connected to `b` with multiple edges of type R
         * then all we need to do is return M's nnz.
         * Otherwise create a new matrix A, where A[i,j] = x
         * where x is the number of edges in M[i,j]

        /* 
           Instead of a user-defined select op, rely on entry <
           MSB_MASK --> multiple relations, and entry >= MSB_MASK -->
           one relation.

           This version assumes the relation arrays live on the host
           and are not accessible by the accelerator.  Otherwise the
           original GrB_apply version is better.
        */

        GrB_Index out = 0;
        uint64_t *vals = NULL;

        GrB_Info info; UNUSED(info);
	GrB_Index nrows, ncols, nvals;
	info = GrB_Matrix_nrows(&nrows, M);
        ASSERT(info == GrB_SUCCESS);
	info = GrB_Matrix_ncols(&ncols, M);
        ASSERT(info == GrB_SUCCESS);
	info = GrB_Matrix_ncols(&nvals, M);
        ASSERT(info == GrB_SUCCESS);

	GrB_Matrix A;
	GrB_Matrix_new(&A, GrB_UINT64, nrows, ncols);
        GxB_Scalar edge_msb;
        info = GxB_Scalar_new (&edge_msb, GrB_UINT64);
        ASSERT(info == GrB_SUCCESS);
        info = GxB_Scalar_setElement (edge_msb, MSB_MASK);
        ASSERT(info == GrB_SUCCESS);

        info = GxB_select (A, GrB_NULL, GrB_NULL, GxB_GE_THUNK, M, edge_msb, GrB_NULL);
        ASSERT(info == GrB_SUCCESS);
        info = GrB_Matrix_nvals (&out, A); // all single edges
        ASSERT(info == GrB_SUCCESS);

        if (out == nvals) goto done; // only single edges
        GrB_Index nvals2;

        info = GxB_select (A, GrB_NULL, GrB_NULL, GxB_LT_THUNK, M, edge_msb, GrB_DESC_R);
        ASSERT(info == GrB_SUCCESS);

        info = GrB_Matrix_nvals (&nvals, A);
        ASSERT (nvals != 0);

        vals = (uint64_t*)array_newlen (uint64_t, nvals);
        nvals2 = nvals;
        info = GrB_Matrix_extractTuples (GrB_NULL, GrB_NULL, vals, &nvals2, A);
        ASSERT(nvals == nvals2);

        OMP(parallel for reduce(+: out))
             for (size_t k = 0; k < nvals2; ++k)
                  out += array_len ((uint64_t*)vals[k]);
done:
        array_free (vals);
        GrB_free (&A);
        return out;
}

void _reduceEdgeCount(ExecutionPlan *plan) {
	/* We'll only modify execution plan if it is structured as follows:
	 * "Full Scan -> Conditional Traverse -> Aggregate -> Results" */
	OpBase *opScan;
	OpBase *opTraverse;
	OpResult *opResult;
	OpAggregate *opAggregate;

	/* See if execution-plan matches the pattern:
	 * "Full Scan -> Conditional Traverse -> Aggregate -> Results".
	 * if that's not the case, simply return without making any modifications. */
	if(!_identifyEdgeCountPattern(plan->root, &opResult, &opAggregate, &opTraverse, &opScan)) return;

	/* User is trying to count edges (either in total or of specific types) in the graph.
	 * Optimize by skipping Scan, Traverse and Aggregate. */
	SIValue edgeCount = SI_LongVal(0);
	Graph *g = QueryCtx_GetGraph();

	// If type is specified, count only labeled entities.
	OpCondTraverse *condTraverse = (OpCondTraverse *)opTraverse;
	// The traversal op doesn't contain information about the traversed edge, cannot apply optimization.
	if(!condTraverse->edge_ctx) return;

	uint edgeRelationCount = array_len(condTraverse->edge_ctx->edgeRelationTypes);

	uint64_t edges = 0;
	for(uint i = 0; i < edgeRelationCount; i++) {
		int relType = condTraverse->edge_ctx->edgeRelationTypes[i];
		switch(relType) {
		case GRAPH_NO_RELATION:
			// Should be the only relationship type mentioned, -[]->
			edges = Graph_EdgeCount(g);
			break;
		case GRAPH_UNKNOWN_RELATION:
			// No change to current count, -[:none_existing]->
			break;
		default:
			edges += _countRelationshipEdges(Graph_GetRelationMatrix(g, relType));
		}
	}
	edgeCount = SI_LongVal(edges);

	/* Construct a constant expression, used by a new
	 * projection operation. */
	AR_ExpNode *exp = AR_EXP_NewConstOperandNode(edgeCount);
	// The new expression must be aliased to populate the Record.
	exp->resolved_name = opAggregate->aggregate_exps[0]->resolved_name;
	AR_ExpNode **exps = array_new(AR_ExpNode *, 1);
	exps = array_append(exps, exp);

	OpBase *opProject = NewProjectOp(opAggregate->op.plan, exps);

	// New execution plan: "Project -> Results"
	ExecutionPlan_RemoveOp(plan, opScan);
	OpBase_Free(opScan);

	ExecutionPlan_RemoveOp(plan, (OpBase *)opTraverse);
	OpBase_Free(opTraverse);

	ExecutionPlan_RemoveOp(plan, (OpBase *)opAggregate);
	OpBase_Free((OpBase *)opAggregate);

	ExecutionPlan_AddOp((OpBase *)opResult, opProject);
}

void reduceCount(ExecutionPlan *plan) {
	/* both _reduceNodeCount and _reduceEdgeCount should count nodes or edges, respectively,
	 * out of the same execution plan.
	 * If node count optimization was unable to execute,
	 * meaning that the execution plan does not hold any node count pattern,
	 * then edge count will be tried to be executed upon the same execution plan */
	if(!_reduceNodeCount(plan)) _reduceEdgeCount(plan);
}

