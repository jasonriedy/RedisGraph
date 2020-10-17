//------------------------------------------------------------------------------
// GxB_MatrixTupleIter:  Iterates over all none zero values of a matrix
//------------------------------------------------------------------------------

typedef struct GBr_MatrixTupleIter_opaque* GxB_MatrixTupleIter;

// Create a new matrix iterator
GrB_Info GxB_MatrixTupleIter_new
(
    GxB_MatrixTupleIter *iter, // iterator to create
    GrB_Matrix A                // matrix being iterated
) ;

// Move iterator over specific rows range
GrB_Info GxB_MatrixTupleIter_iterate_range
(
    GxB_MatrixTupleIter iter,  // iterator to use
    GrB_Index startRowIdx,      // row index to start with
    GrB_Index endRowIdx         // row index to finish with
) ;

GrB_Info 
GxB_MatrixTupleIter_iterate_row (GxB_MatrixTupleIter iter, GrB_Index rowIdx);

// Advance iterator to the next none zero value
GrB_Info GxB_MatrixTupleIter_next
(
    GxB_MatrixTupleIter iter,      // iterator to consume
    GrB_Index *row,                 // optional row index of current NNZ
    GrB_Index *col,                 // optional column index of current NNZ
    bool *depleted                  // indicate if iterator depleted
) ;

// Reset iterator
GrB_Info GxB_MatrixTupleIter_reset
(
    GxB_MatrixTupleIter iter        // iterator to reset
) ;

// Reuse iterator to scan given matrix
GrB_Info GxB_MatrixTupleIter_reuse
(
    GxB_MatrixTupleIter iter,       // iterator to update
    GrB_Matrix A            // matrix to scan
) ;

// Release every resource consumed by iterator
GrB_Info GxB_MatrixTupleIter_free
(
    GxB_MatrixTupleIter *iter        // iterator to free
) ;
