#if !defined(GxB_Matrix_tuple_iter_H_)
#define GxB_Matrix_tuple_iter_H_
//------------------------------------------------------------------------------
// GxB_MatrixTupleIter:  Iterates over all none zero values of a matrix
//------------------------------------------------------------------------------

typedef struct GBr_MatrixTupleIter_opaque* GxB_MatrixTupleIter;

// Create a new matrix iterator
GrB_Info GxB_MatrixTupleIter_new
(
    GxB_MatrixTupleIter *gxb_iter, // iterator to create
    GrB_Matrix A                // matrix being iterated
) ;

// Iterate over specific row
GrB_Info GxB_MatrixTupleIter_iterate_row
(
    GxB_MatrixTupleIter gxb_iter,  // iterator to use
    GrB_Index rowIdx            // row index to iterate over
) ;

// Move iterator over specific rows range
GrB_Info GxB_MatrixTupleIter_iterate_range
(
    GxB_MatrixTupleIter gxb_iter,  // iterator to use
    GrB_Index startRowIdx,      // row index to start with
    GrB_Index endRowIdx         // row index to finish with
) ;


// Advance iterator to the next none zero value
GrB_Info GxB_MatrixTupleIter_next
(
    GxB_MatrixTupleIter gxb_iter,      // iterator to consume
    GrB_Index *row,                 // optional row index of current NNZ
    GrB_Index *col,                 // optional column index of current NNZ
    bool *depleted                  // indicate if iterator depleted
) ;

// Reset iterator
GrB_Info GxB_MatrixTupleIter_reset
(
    GxB_MatrixTupleIter gxb_iter        // iterator to reset
) ;

// Reuse iterator to scan given matrix
GrB_Info GxB_MatrixTupleIter_reuse
(
    GxB_MatrixTupleIter gxb_iter,       // iterator to update
    GrB_Matrix A            // matrix to scan
) ;

// Release every resource consumed by iterator
GrB_Info GxB_MatrixTupleIter_free
(
    GxB_MatrixTupleIter *gxb_iter        // iterator to free
) ;
#endif
