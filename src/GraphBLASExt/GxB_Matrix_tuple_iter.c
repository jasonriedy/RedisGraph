//------------------------------------------------------------------------------
// GxB_MatrixTupleIter: Iterates over matrix none zero values
//------------------------------------------------------------------------------

#include <stdlib.h>
#include <stdint.h>

#include "../RG.h"
#include "../../deps/GraphBLAS/Include/GraphBLAS.h"

// Not great, but handy.
#define IDX_ALL ((uint64_t)-1)

struct GBr_MatrixTupleIter_opaque {
     GrB_Matrix A;
     GrB_Index nr, nc, n;
     GrB_Descriptor desc;

     GrB_Index begin, end;
     GrB_Index current, offset;

     GrB_Vector v;
     GrB_Index v_pattern_len, *v_pattern, v_pattern_storage_len;
};

#include "GxB_Matrix_tuple_iter.h"

static GrB_Info iter_init (struct GBr_MatrixTupleIter_opaque *gxb_iter, GrB_Matrix A, GrB_Index begin, GrB_Index end);

GrB_Info 
GxB_MatrixTupleIter_free (GxB_MatrixTupleIter *gxb_iter)
{
     if (gxb_iter == NULL) return GrB_NULL_POINTER;
     struct GBr_MatrixTupleIter_opaque *iter = *gxb_iter;
     if (iter == NULL) return GrB_SUCCESS;
     GrB_Info info;
     free (iter->v_pattern);
     info = GrB_free (&iter->v);
     *gxb_iter = NULL;
     memset (iter, 0, sizeof (*iter));
     return info;
}

GrB_Info 
GxB_MatrixTupleIter_new (GxB_MatrixTupleIter *gxb_iter, GrB_Matrix A)
{
     if (gxb_iter == NULL) return GrB_NULL_POINTER;
     
     struct GBr_MatrixTupleIter_opaque *iter;
     
     iter = calloc (1, sizeof(struct GBr_MatrixTupleIter_opaque));
     if (iter == NULL)
          return GrB_OUT_OF_MEMORY;
     
     GrB_Info info = iter_init (iter, A, 0, IDX_ALL);
     if (info != GrB_SUCCESS) free (iter);
     *gxb_iter = iter;
     return info;
}

GrB_Info 
GxB_MatrixTupleIter_reuse (GxB_MatrixTupleIter gxb_iter, GrB_Matrix A)
{
     struct GBr_MatrixTupleIter_opaque *iter = gxb_iter;
     if (iter == NULL) return GrB_NULL_POINTER;

     return iter_init (iter, A, 0, IDX_ALL);
}

GrB_Info 
GxB_MatrixTupleIter_reset (GxB_MatrixTupleIter gxb_iter)
{
     struct GBr_MatrixTupleIter_opaque *iter = gxb_iter;
     if (iter == NULL) return GrB_NULL_POINTER;
     return GxB_MatrixTupleIter_reuse (gxb_iter, iter->A);
}

GrB_Info 
GxB_MatrixTupleIter_iterate_range (GxB_MatrixTupleIter gxb_iter, GrB_Index startRowIdx, GrB_Index endRowIdx)
{
     struct GBr_MatrixTupleIter_opaque *iter = gxb_iter;
     if (iter == NULL) return GrB_NULL_POINTER;

     if (startRowIdx > endRowIdx) return GrB_INVALID_INDEX;

     return iter_init (iter, iter->A, startRowIdx, (endRowIdx != IDX_ALL? endRowIdx+1 : endRowIdx));
}

GrB_Info 
GxB_MatrixTupleIter_iterate_row (GxB_MatrixTupleIter iter, GrB_Index rowIdx)
{
     return GxB_MatrixTupleIter_iterate_range (iter, rowIdx, rowIdx);
}

static GrB_Info
iter_forward_from (struct GBr_MatrixTupleIter_opaque *iter, GrB_Index begin)
// Stops at the first non-empty column in [begin, iter->end).
{
     ASSERT (iter != NULL);
     const GrB_Matrix A = iter->A;
     const GrB_Index end = iter->end; 
     const GrB_Index n = iter->n; // n and desc abstract out treating a mx1 matrix as a row vector.
     const GrB_Descriptor desc = iter->desc;
     GrB_Vector v = iter->v;
     GrB_Index v_nv;
     GrB_Info info = GrB_SUCCESS;

     ASSERT (iter->offset == iter->v_pattern_len); // Requires both to be zero on initialization / reuse.

     if (begin == end) goto info_out;

     v_nv = 0;
     do {
          info = GrB_extract (v, GrB_NULL, GrB_NULL, A, GrB_ALL, n, begin, desc);
          if (info != GrB_SUCCESS) goto info_out;
          info = GrB_Vector_nvals (&v_nv, v);
          if (info != GrB_SUCCESS) goto info_out;
          if (v_nv != 0) break;
          ++begin;
     } while (begin < end);
     if (begin == end) {
          GrB_Index nvals;
          GrB_Matrix_nvals (&nvals, iter->A);
          goto info_out; // No more entries, so "clean up" and report success.
     }
     
     // begin holds the current row index, and v holds the current row.

     // Allocate space if needed.
     if (v_nv > iter->v_pattern_storage_len) {
          GrB_Index *tmp = realloc (iter->v_pattern, v_nv * sizeof (*iter->v_pattern));
          if (tmp == NULL) return GrB_OUT_OF_MEMORY;
          iter->v_pattern = tmp;
          iter->v_pattern_storage_len = v_nv;
     }

     GrB_Index n_needed_extracted = v_nv;

     info = GrB_Vector_extractTuples (iter->v_pattern, (bool*)GrB_NULL, &n_needed_extracted, v);
     if (info != GrB_SUCCESS) goto info_out;
     if (v_nv != n_needed_extracted) { info = GrB_DIMENSION_MISMATCH; goto info_out; }

     // v is a handle / reference and already has been updated.
     iter->offset = 0;
     iter->current = begin;

     iter->v_pattern_len = v_nv;

     return GrB_SUCCESS;
     
info_out:
     // Ensure any future next() calls set depleted.
     iter->current = end;
     iter->v_pattern_len = 0;
     iter->offset = 0;
     return info;
}

GrB_Info
iter_init (struct GBr_MatrixTupleIter_opaque *iter, GrB_Matrix A, GrB_Index begin, GrB_Index end)
{
     GrB_Index nr, nc;
     GrB_Info info;
     
     // Find first non-empty row in [begin, end).
     GrB_Vector v;

     GrB_Index n;
     GrB_Descriptor desc;
     GrB_Type elt_type;

     if (iter == NULL) return GrB_NULL_POINTER;
     info = GrB_Matrix_ncols (&nc, A);
     if (info != GrB_SUCCESS) return info;
     info = GrB_Matrix_nrows (&nr, A);
     if (info != GrB_SUCCESS) return info;
     info = GxB_Matrix_type (&elt_type, A);

     if (nc == 1) {   
          // Column vector, will treat as a row.
          // Must be *one* row requested, the entire vector.
          if (begin != 0 || !(end == 1 || end == IDX_ALL))
               return GrB_INDEX_OUT_OF_BOUNDS;
          end = 1; // deal with IDX_ALL
          n = nr;
          desc = GrB_NULL;
     } else {
          if (begin >= nr || (end > nr && end != IDX_ALL))
               return GrB_INDEX_OUT_OF_BOUNDS;
          if (end == IDX_ALL) end = nr;
          n = nc;
          desc = GrB_DESC_T0;
     }

     if (iter->v == NULL) {
          info = GrB_Vector_new (&v, elt_type, n);
     } else { // Re-use old one if possible...
          v = iter->v;
          GrB_Type v_type;
          info = GxB_Vector_type (&v_type, v);
          if (info != GrB_SUCCESS) return info;
          if (v_type == elt_type)
               info = GxB_Vector_resize (v, n);
          else
               info = GrB_Vector_new (&v, elt_type, n);
     }
     if (info != GrB_SUCCESS) return info;

     *iter = (struct GBr_MatrixTupleIter_opaque){ .A = A, .nr = nr, .nc = nc, 
          .n = n, .desc = desc, .begin = begin, .end = end, .current = end,
          .offset = 0, .v = v, .v_pattern_len = 0};

     info = iter_forward_from (iter, begin);

     if (info != GrB_SUCCESS) {
          GrB_free (&v);
          return info;
     }

     // Either an empty matrix, or pointing at the first entry of a vector.
     ASSERT (iter->current <= iter->end);
     ASSERT (iter->current != iter->end || iter->v_pattern_len == 0);
     ASSERT (iter->current == iter->end || (iter->current >= iter->begin && iter->offset == 0));

     return GrB_SUCCESS;
}

GrB_Info 
GxB_MatrixTupleIter_next (GxB_MatrixTupleIter gxb_iter, GrB_Index *row, GrB_Index *col, bool *depleted)
{
     /* General logic:
        If finished, set depleted to true and return.
        Retrieve and store the current entry (special case column vectors).
        Forward to next entry.
        Set depleted to false.
      */
     struct GBr_MatrixTupleIter_opaque *iter = gxb_iter;
     if (iter == NULL) {
          if (depleted) *depleted = true;
          return GrB_NULL_POINTER;
     }

     // First, check if already depleted.
     if (iter->current == iter->end) {
          if (depleted)  *depleted = true;
          return GrB_SUCCESS;
     }

     GrB_Info info;

     if (iter->offset == iter->v_pattern_len) {
          // The current vector is depleted, so forward.
          info = iter_forward_from (iter, iter->current + 1);
          if (info != GrB_SUCCESS) {
               if (depleted) *depleted = true;
               return info;
          }
     }

     if (iter->current == iter->end) {
          if (depleted) *depleted = true;
          return GrB_SUCCESS;
     }

     if (iter->nc == 1) { // column vector, treated as a row
          if (col) *col = iter->v_pattern[iter->offset];
          if (row) *row = 0;
     } else {
          if (col) *col = iter->v_pattern[iter->offset];
          if (row) *row = iter->current;
     }
     ++iter->offset;
     if (depleted) *depleted = false;
     return GrB_SUCCESS;
}
