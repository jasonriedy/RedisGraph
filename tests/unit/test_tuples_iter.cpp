/*
* Copyright 2018-2020 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "gtest.h"

#ifdef __cplusplus
extern "C" {
#endif

#include "../../deps/GraphBLAS/Include/GraphBLAS.h"
#include "../../src/GraphBLASExt/GxB_Matrix_tuple_iter.h"
#include "../../src/util/rmalloc.h"

#ifdef __cplusplus
}
#endif

class TuplesTest: public ::testing::Test {
  protected:
	static void SetUpTestCase() {
		// Use the malloc family for allocations
		Alloc_Reset();

		GrB_init(GrB_NONBLOCKING);
		GxB_Global_Option_set(GxB_FORMAT, GxB_BY_ROW); // all matrices in CSR format
		GxB_Global_Option_set(GxB_HYPER, GxB_NEVER_HYPER); // matrices are never hypersparse
	}

	static void TearDownTestCase() {
		GrB_finalize();
	}

	GrB_Matrix CreateSquareNByNDiagonalMatrix(GrB_Index n) {
		GrB_Matrix A = CreateSquareNByNEmptyMatrix(n);

		GrB_Index I[n];
		GrB_Index J[n];
		bool X[n];

		// Initialize.
		for(int i = 0; i < n; i++) {
			I[i] = i;
			J[i] = i;
			X[i] = i;
		}

		GrB_Matrix_build_BOOL(A, I, J, X, n, GrB_FIRST_BOOL);

		return A;
	}

	GrB_Matrix CreateSquareNByNEmptyMatrix(GrB_Index n) {
		GrB_Matrix A;
		GrB_Matrix_new(&A, GrB_BOOL, n, n);
		return A;
	}
};

#if 0 // Buggy: Assumes vectors and matrices can be typecast.
TEST_F(TuplesTest, RandomVectorTest) {
	//--------------------------------------------------------------------------
	// Build a random vector
	//--------------------------------------------------------------------------

	GrB_Vector A;
	GrB_Index nvals = 0;
	GrB_Index nrows = 1024;
	GrB_Index *I = (GrB_Index *)malloc(sizeof(GrB_Index) * nrows);
	bool *X = (bool *)malloc(sizeof(bool) * nrows);

	GrB_Vector_new(&A, GrB_BOOL, nrows);

	double mid_point = RAND_MAX / 2;
	for(int i = 0; i < nrows; i++) {
		if(rand() > mid_point) {
			I[nvals] = i;
			X[nvals] = true;
			nvals++;
		}
	}

	GrB_Vector_build_BOOL(A, I, X, nvals, GrB_FIRST_BOOL);

	GrB_Index I_expected[nvals];
	GrB_Vector_extractTuples_BOOL(I_expected, NULL, &nvals, A);

	//--------------------------------------------------------------------------
	// Get an iterator over all nonzero elements.
	//--------------------------------------------------------------------------

	GxB_MatrixTupleIter iter;
        /* XXX: Assumes they use the same structure. */ GxB_MatrixTupleIter_new(&iter, (GrB_Matrix)A);
        ASSERT_FALSE(iter == NULL);
	GrB_Index col;

	//--------------------------------------------------------------------------
	// Verify iterator returned values.
	//--------------------------------------------------------------------------
	bool depleted = false;
	for(int i = 0; i < nvals; i++) {
		GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
		ASSERT_FALSE(depleted);
		ASSERT_EQ(col, I_expected[i]);
	}
	GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
	ASSERT_TRUE(depleted);

	//--------------------------------------------------------------------------
	// Clean up.
	//--------------------------------------------------------------------------
	free(I);
	free(X);
	GxB_MatrixTupleIter_free(&iter);
	GrB_Vector_free(&A);
}

TEST_F(TuplesTest, VectorIteratorTest) {
	//--------------------------------------------------------------------------
	// Build a vector
	//--------------------------------------------------------------------------

	GrB_Vector A;
	GrB_Vector_new(&A, GrB_BOOL, 4);

	GrB_Index nvals = 2;
	GrB_Index I[2] = {1, 3};
	bool X[2] = {true, true};
	GrB_Index I_expected[nvals];

	GrB_Vector_build_BOOL(A, I, X, nvals, GrB_FIRST_BOOL);
	GrB_Vector_extractTuples_BOOL(I_expected, NULL, &nvals, A);

	//--------------------------------------------------------------------------
	// Get an iterator over all vector nonzero elements.
	//--------------------------------------------------------------------------

	GxB_MatrixTupleIter iter;
	/* XXX: Assumes they use the same structure. */ GxB_MatrixTupleIter_new(&iter, (GrB_Matrix)A);
	GrB_Index col;

	//--------------------------------------------------------------------------
	// Verify iterator returned values.
	//--------------------------------------------------------------------------
	bool depleted = false;
	for(int i = 0; i < nvals; i++) {
		GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
		ASSERT_FALSE(depleted);
		ASSERT_EQ(col, I_expected[i]);
	}
	GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
	ASSERT_TRUE(depleted);

	//--------------------------------------------------------------------------
	// Reset iterator and re-verify.
	//--------------------------------------------------------------------------

	GxB_MatrixTupleIter_reset(iter);
	for(int i = 0; i < nvals; i++) {
		GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
		ASSERT_FALSE(depleted);
		ASSERT_EQ(col, I_expected[i]);
	}
	GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
	ASSERT_TRUE(depleted);

	//--------------------------------------------------------------------------
	// Clean up.
	//--------------------------------------------------------------------------
	GxB_MatrixTupleIter_free(&iter);
	GrB_Vector_free(&A);
}
#endif // Buggy

TEST_F(TuplesTest, RandomMatrixTest) {
	//--------------------------------------------------------------------------
	// Build a random matrix
	//--------------------------------------------------------------------------

     GrB_Matrix A, B, C;
	GrB_Index nvals = 0;
	GrB_Index nrows = 32;//1024;
	GrB_Index ncols = 32;//1024;
	GrB_Index *I = (GrB_Index *)malloc(sizeof(GrB_Index) * ncols * nrows);
	GrB_Index *J = (GrB_Index *)malloc(sizeof(GrB_Index) * ncols * nrows);
	bool *X = (bool *)malloc(sizeof(bool) * ncols * nrows);

	GrB_Matrix_new(&A, GrB_BOOL, nrows, ncols);

	double mid_point = RAND_MAX / 2;
	for(int i = 0; i < nrows; i++) {
		for(int j = 0; j < ncols; j++) {
			if(rand() > mid_point) {
				I[nvals] = i;
				J[nvals] = j;
				X[nvals] = true;
				nvals++;
			}
		}
	}
	GrB_Matrix_build_BOOL(A, I, J, X, nvals, GrB_FIRST_BOOL);

	//--------------------------------------------------------------------------
	// Get an iterator over all matrix explicity stored elements.
	//--------------------------------------------------------------------------

	GxB_MatrixTupleIter iter;
	GxB_MatrixTupleIter_new(&iter, A);
	GrB_Index row;
	GrB_Index col;

        memset (I, 0, ncols * nrows * sizeof(*I));
        memset (J, 0, ncols * nrows * sizeof(*J));
        memset (X, 0, ncols * nrows * sizeof(*X));

	//--------------------------------------------------------------------------
	// Verify iterator returned values.
	//--------------------------------------------------------------------------
	bool depleted = false;
	for(int i = 0; i < nvals; i++) {
		GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
		ASSERT_FALSE(depleted);
                I[i] = row;
                J[i] = col;
                X[i] = true;
	}
	GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
	ASSERT_TRUE(depleted);

	GrB_Matrix_new(&B, GrB_BOOL, nrows, ncols);
	GrB_Matrix_build_BOOL(B, I, J, X, nvals, GrB_FIRST_BOOL);

	GrB_Matrix_new(&C, GrB_BOOL, nrows, ncols);
        GrB_eWiseAdd_Matrix_BinaryOp(C, GrB_NULL, GrB_NULL, GxB_LXOR_BOOL, A, B, GrB_NULL);
        // C's stored entries are false where A == B and true where they differ.

        GrB_Index nvals_check;
        GrB_Matrix_nvals (&nvals_check, C);
        ASSERT_EQ(nvals, nvals_check);
        bool any_mismatches = false;
        GrB_Matrix_reduce_BOOL(&any_mismatches, GrB_NULL, GxB_LOR_BOOL_MONOID, C, GrB_NULL);
        ASSERT_FALSE(any_mismatches);
	//--------------------------------------------------------------------------
	// Clean up.
	//--------------------------------------------------------------------------
	free(I);
	free(J);
	free(X);
	GxB_MatrixTupleIter_free(&iter);
	GrB_Matrix_free(&C);
	GrB_Matrix_free(&B);
	GrB_Matrix_free(&A);
}

#if 0 // fix later
TEST_F(TuplesTest, MatrixIteratorTest) {
	//--------------------------------------------------------------------------
	// Build a 4X4 matrix
	//--------------------------------------------------------------------------

	GrB_Index nvals = 4;
	GrB_Matrix A = CreateSquareNByNDiagonalMatrix(nvals);
	GrB_Index I_expected[nvals];
	GrB_Index J_expected[nvals];
	GrB_Matrix_extractTuples_BOOL(I_expected, J_expected, NULL, &nvals, A);

	//--------------------------------------------------------------------------
	// Get an iterator over all matrix nonzero elements.
	//--------------------------------------------------------------------------

	GxB_MatrixTupleIter iter;
	GxB_MatrixTupleIter_new(&iter, A);
	GrB_Index row;
	GrB_Index col;

	//--------------------------------------------------------------------------
	// Verify iterator returned values.
	//--------------------------------------------------------------------------
	bool depleted = false;
	for(int i = 0; i < nvals; i++) {
		GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
		ASSERT_FALSE(depleted);
		ASSERT_EQ(row, I_expected[i]);
		ASSERT_EQ(col, J_expected[i]);
	}
	GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
	ASSERT_TRUE(depleted);

	//--------------------------------------------------------------------------
	// Reset iterator an re-verify.
	//--------------------------------------------------------------------------

	GxB_MatrixTupleIter_reset(iter);
	for(int i = 0; i < nvals; i++) {
		GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
		ASSERT_FALSE(depleted);
		ASSERT_EQ(row, I_expected[i]);
		ASSERT_EQ(col, J_expected[i]);
	}
	GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
	ASSERT_TRUE(depleted);

	//--------------------------------------------------------------------------
	// Clean up.
	//--------------------------------------------------------------------------
	GxB_MatrixTupleIter_free(&iter);
	GrB_Matrix_free(&A);
}

TEST_F(TuplesTest, IteratorRange) {

	// Matrix is 6X6 and will be populated with the following indices.
	GrB_Index indices[6][2] = {
		{0, 2},
		{2, 1},
		{2, 3},
		{3, 0},
		{3, 4},
		{5, 5}
	};

	bool depleted;
	GrB_Info info;
	GrB_Index row;
	GrB_Index col;

	// Create and populate the matrix.
	GrB_Index n = 6;
	GrB_Matrix A = CreateSquareNByNEmptyMatrix(n);
	for(int i = 0; i < 6; i ++) {
		row = indices[i][0];
		col = indices[i][1];
		GrB_Matrix_setElement_BOOL(A, true, row, col);
	}

	// Create iterator.
	GxB_MatrixTupleIter iter;
	GxB_MatrixTupleIter_new(&iter, A);

	// Check for invalid index exception for range iteration.
	info = GxB_MatrixTupleIter_iterate_range(iter, -1, n - 1);
	ASSERT_EQ(GrB_INVALID_INDEX, info);
	info = GxB_MatrixTupleIter_iterate_range(iter, n - 1, 0);
	ASSERT_EQ(GrB_INVALID_INDEX, info);
	// Check for invalid index exception on out-of-bounds iterator.
	info = GxB_MatrixTupleIter_iterate_range(iter, n + 5, n + 5);
	ASSERT_EQ(GrB_INVALID_INDEX, info);

	// Iterate single row.
	info = GxB_MatrixTupleIter_iterate_range(iter, 2, 2);
	ASSERT_EQ(GrB_SUCCESS, info);

	// Check that the right indices are retrived.
	for(int i = 1; i <= 2; i++) {
		info = GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
		ASSERT_EQ(GrB_SUCCESS, info);
		ASSERT_EQ(indices[i][0], row);
		ASSERT_EQ(indices[i][1], col);
		ASSERT_FALSE(depleted);
	}
	GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
	ASSERT_TRUE(depleted);

	// Check for legal range setting.
	info = GxB_MatrixTupleIter_iterate_range(iter, 2, 3);
	ASSERT_EQ(GrB_SUCCESS, info);

	// Check that the right indices are retrived.
	for(int i = 1; i <= 4; i++) {
		info = GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
		ASSERT_EQ(GrB_SUCCESS, info);
		ASSERT_EQ(indices[i][0], row);
		ASSERT_EQ(indices[i][1], col);
		ASSERT_FALSE(depleted);
	}
	GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
	ASSERT_TRUE(depleted);


	// Set the entire rows as range, check that iterator is depleted only when it is done iterating the matrix.
	info = GxB_MatrixTupleIter_iterate_range(iter, 0, n - 1);
	ASSERT_EQ(GrB_SUCCESS, info);

	for(int i = 0; i < 6; i ++) {
		info = GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
		ASSERT_EQ(GrB_SUCCESS, info);
		ASSERT_EQ(indices[i][0], row);
		ASSERT_EQ(indices[i][1], col);
		ASSERT_FALSE(depleted);
	}
	GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
	ASSERT_TRUE(depleted);
}
#endif // fix later
