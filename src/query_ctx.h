/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#pragma once

#include"ast/ast.h"
#include "redismodule.h"
#include "util/rmalloc.h"
#include "graph/graphcontext.h"
#include <setjmp.h>
#include <pthread.h>
#include "commands/cmd_context.h"

extern pthread_key_t _tlsQueryCtxKey;  // Thread local storage query context key.

typedef struct {
	AST *ast;       // The scoped AST associated with this query.
	rax *params;    // Query parameters.
	const char *query;    // Query string.
} QueryCtx_QueryData;

typedef struct {
	char *error;            // The error message produced by this query, if any.
	double timer[2];        // Query execution time tracking.
	jmp_buf *breakpoint;    // The breakpoint to return to if the query causes an exception.
	RedisModuleKey *key;    // Saves an open key value, for later extraction and closing.
} QueryCtx_InternalExecCtx;

typedef struct {
	RedisModuleCtx *redis_ctx;      // The Redis module context.
	RedisModuleBlockedClient *bc;   // Blocked client.
	const char *command_name;       // Command name.
} QueryCtx_GlobalExecCtx;

typedef struct {
	QueryCtx_QueryData query_data;              // The data related to the query syntax.
	QueryCtx_InternalExecCtx internal_exec_ctx; // The data related to internal query execution.
	QueryCtx_GlobalExecCtx global_exec_ctx;     // The data rlated to global redis execution.
	GraphContext *gc;                           // The GraphContext associated with this query's graph.
} QueryCtx;

/* On invocation, set an exception handler, returning 0 from this macro.
 * Upon encountering an exception, execution will resume at this point and return nonzero. */
#define SET_EXCEPTION_HANDLER()                                         \
   ({                                                                   \
	QueryCtx *ctx = pthread_getspecific(_tlsQueryCtxKey);               \
	if(!ctx->internal_exec_ctx.breakpoint) ctx->internal_exec_ctx.breakpoint = rm_malloc(sizeof(jmp_buf));  \
	setjmp(*ctx->internal_exec_ctx.breakpoint);                                           \
})

/* Instantiate the thread-local QueryCtx on module load. */
bool QueryCtx_Init(void);
/* Free the thread-local QueryCtx variable (unused). */
void QueryCtx_Finalize(void);
/* Start timing query execution. */
void QueryCtx_BeginTimer(void);

/* Jump to a runtime exception breakpoint if one has been set. */
void QueryCtx_RaiseRuntimeException(void);
/* Reply back to the user with error. */
void QueryCtx_EmitException(void);

/* Setters */
/* Sets the global execution context */
void QueryCtx_SetGlobalExecutionCtx(CommandCtx *cmd_ctx);
/* Set the provided AST for access through the QueryCtx. */
void QueryCtx_SetAST(AST *ast);
/* Set the error message for this query. */
void QueryCtx_SetError(char *error);
/* Set the provided GraphCtx for access through the QueryCtx. */
void QueryCtx_SetGraphCtx(GraphContext *gc);
/* Set the Redis module context. */
void QueryCtx_SetRedisModuleCtx(RedisModuleCtx *redisctx);

/* Getters */
/* Retrieve the AST. */
AST *QueryCtx_GetAST(void);
/* Retrive the query parameters values map. */
rax *QueryCtx_GetParams(void);
/* Retrieve the Graph object. */
Graph *QueryCtx_GetGraph(void);
/* Retrieve the GraphCtx. */
GraphContext *QueryCtx_GetGraphCtx(void);
/* Retrieve the Redis module context. */
RedisModuleCtx *QueryCtx_GetRedisModuleCtx(void);

/* Starts a locking flow before commiting changes in the graph and Redis keyspace.
 * Locking flow is:
 * 1. LOCK GIL
 * 2. Key open with `write` flag
 * 3. Graph R/W lock with write flag
 * This method returns false if the key has changed from the current graph,
 * and sets the relevant error message. */
bool QueryCtx_LockForCommit(void);

/* Starts an ulocking flow and notifies Redis after commiting changes in the graph and Redis keyspace.
 * Unlocking flow is:
 * 1. Replicate.
 * 2. Unlock graph R/W lock
 * 3. Close key
 * 4. Unlock GIL */
void QueryCtx_UnlockCommit(void);

/* Compute and return elapsed query execution time. */
double QueryCtx_GetExecutionTime(void);
/* Returns true if this query has caused an error. */
bool QueryCtx_EncounteredError(void);
/* Free the allocations within the QueryCtx and reset it for the next query. */
void QueryCtx_Free(void);
