#if !defined(GxB_Matrix_tuple_iter_H_)
#define GxB_Matrix_tuple_iter_H_
//------------------------------------------------------------------------------
// GxB_MatrixTupleIter:  Iterates over all none zero values of a matrix
//------------------------------------------------------------------------------

// TuplesIter maintains information required
// to iterate over a matrix
typedef struct
{
    GrB_Matrix A ;          // Matrix being iterated
    GrB_Index nvals ;       // Number of none zero values in matrix
    GrB_Index nnz_idx ;     // Index of current none zero value
    int64_t p ;             // Number of none zero values in current column
    int64_t row_idx ;       // Index of current row
    GrB_Index nrows ;       // Total number of rows in matrix
} GxB_MatrixTupleIter ;

// Create a new matrix iterator
GrB_Info GxB_MatrixTupleIter_new
(
    GxB_MatrixTupleIter **iter, // iterator to create
    GrB_Matrix A                // matrix being iterated
) ;

// Iterate over specific row
GrB_Info GxB_MatrixTupleIter_iterate_row
(
    GxB_MatrixTupleIter *iter,  // iterator to use
    GrB_Index rowIdx            // row index to iterate over
) ;

// Move iterator to a specific row
GrB_Info GxB_MatrixTupleIter_jump_to_row
(
    GxB_MatrixTupleIter *iter,  // iterator to use
    GrB_Index rowIdx            // row index to move iterator to
) ;

// Move iterator over specific rows range
GrB_Info GxB_MatrixTupleIter_iterate_range
(
    GxB_MatrixTupleIter *iter,  // iterator to use
    GrB_Index startRowIdx,      // row index to start with
    GrB_Index endRowIdx         // row index to finish with
) ;


// Advance iterator to the next none zero value
GrB_Info GxB_MatrixTupleIter_next
(
    GxB_MatrixTupleIter *iter,      // iterator to consume
    GrB_Index *row,                 // optional row index of current NNZ
    GrB_Index *col,                 // optional column index of current NNZ
    bool *depleted                  // indicate if iterator depleted
) ;

// Reset iterator
GrB_Info GxB_MatrixTupleIter_reset
(
    GxB_MatrixTupleIter *iter        // iterator to reset
) ;

// Reuse iterator to scan given matrix
GrB_Info GxB_MatrixTupleIter_reuse
(
    GxB_MatrixTupleIter *iter,       // iterator to update
    GrB_Matrix A            // matrix to scan
) ;

// Release every resource consumed by iterator
GrB_Info GxB_MatrixTupleIter_free
(
    GxB_MatrixTupleIter *iter        // iterator to free
) ;
#endif
