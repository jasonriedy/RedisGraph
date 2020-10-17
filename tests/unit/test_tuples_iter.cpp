/*
* Copyright 2018-2020 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include <iostream>
#include <algorithm>

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

     bool check_eq_bool (const GrB_Vector A, const GrB_Index n, const GrB_Index *I, const GrB_Index nvals)
          {
               GrB_Vector B, C;
               bool *X = new bool[nvals];
               bool out;

               std::fill_n (X, nvals, true);
               GrB_Vector_new (&B, GrB_BOOL, n);
               GrB_Vector_new (&C, GrB_BOOL, n);
               GrB_Vector_build_BOOL(B, I, X, nvals, GrB_FIRST_BOOL);

               GrB_eWiseAdd_Vector_Monoid (C, GrB_NULL, GrB_NULL, GxB_LXOR_BOOL_MONOID, A, B, GrB_NULL);

               GrB_Vector_reduce_BOOL (&out, GrB_NULL, GxB_LOR_BOOL_MONOID, C, GrB_NULL);

               delete[] X;
               GrB_Vector_free (&C);
               GrB_Vector_free (&B);

               return !out;
          }
     bool check_eq_bool (const GrB_Matrix A, const GrB_Index nr, const GrB_Index nc, 
                         const GrB_Index *I, const GrB_Index *J, const GrB_Index nvals)
          {
               GrB_Matrix B, C;
               bool *X = new bool[nvals];
               bool out;

               std::fill_n (X, nvals, true);
               GrB_Matrix_new (&B, GrB_BOOL, nr, nc);
               GrB_Matrix_new (&C, GrB_BOOL, nr, nc);
               GrB_Matrix_build_BOOL(B, I, J, X, nvals, GrB_FIRST_BOOL);

               GrB_eWiseAdd_Matrix_Monoid (C, GrB_NULL, GrB_NULL, GxB_LXOR_BOOL_MONOID, A, B, GrB_NULL);

               GrB_Matrix_reduce_BOOL (&out, GrB_NULL, GxB_LOR_BOOL_MONOID, C, GrB_NULL);

               delete[] X;
               GrB_Matrix_free (&C);
               GrB_Matrix_free (&B);

               return !out;
          }
};

TEST_F(TuplesTest, RandomVectorTest) {
	//--------------------------------------------------------------------------
	// Build a random vector
	//--------------------------------------------------------------------------

        GrB_Vector A;
	GrB_Index nvals = 0;
	GrB_Index nrows = 1024;
	GrB_Index *I = new GrB_Index[nrows];
	bool *X = new bool[nrows];

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

	GrB_Index *I_iter;
        I_iter = new GrB_Index[nvals];

	//--------------------------------------------------------------------------
	// Get an iterator over all nonzero elements.
	//--------------------------------------------------------------------------

	GxB_MatrixTupleIter iter;
	GxB_MatrixTupleIter_new(&iter, (GrB_Matrix)A); // XXX: Incorrect.
	GrB_Index col;

	//--------------------------------------------------------------------------
	// Verify iterator returned values.
	//--------------------------------------------------------------------------
	bool depleted = false;
	for(int i = 0; i < nvals; i++) {
		GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
                I_iter[i] = col;
		ASSERT_FALSE(depleted);
	}
	GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
	ASSERT_TRUE(depleted);

        bool out = check_eq_bool (A, nrows, I_iter, nvals);
        ASSERT_TRUE(out);

	//--------------------------------------------------------------------------
	// Clean up.
	//--------------------------------------------------------------------------
        delete[] I_iter;
	delete[] I;
        delete[] X;
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

	GrB_Vector_build_BOOL(A, I, X, nvals, GrB_FIRST_BOOL);

	GrB_Index *I_iter;
        I_iter = new GrB_Index[nvals];

	//--------------------------------------------------------------------------
	// Get an iterator over all vector nonzero elements.
	//--------------------------------------------------------------------------

	GxB_MatrixTupleIter iter;
	GxB_MatrixTupleIter_new(&iter, (GrB_Matrix)A); // XXX: Incorrect
	GrB_Index col;

	//--------------------------------------------------------------------------
	// Verify iterator returned values.
	//--------------------------------------------------------------------------
	bool depleted = false;
	for(int i = 0; i < nvals; i++) {
		GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
		ASSERT_FALSE(depleted);
                I_iter[i] = col;
	}
	GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
	ASSERT_TRUE(depleted);

        bool out = check_eq_bool (A, 2, I_iter, nvals);
        ASSERT_TRUE(out);

	//--------------------------------------------------------------------------
	// Reset iterator and re-verify.
	//--------------------------------------------------------------------------

	GxB_MatrixTupleIter_reset(iter);
	for(int i = 0; i < nvals; i++) {
		GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
		ASSERT_FALSE(depleted);
                I_iter[i] = col;
	}
	GxB_MatrixTupleIter_next(iter, NULL, &col, &depleted);
	ASSERT_TRUE(depleted);

        out = check_eq_bool (A, 2, I_iter, nvals);
        ASSERT_TRUE(out);

	//--------------------------------------------------------------------------
	// Clean up.
	//--------------------------------------------------------------------------
        delete[] I_iter;
	GxB_MatrixTupleIter_free(&iter);
	GrB_Vector_free(&A);
}

TEST_F(TuplesTest, RandomMatrixTest) {
	//--------------------------------------------------------------------------
	// Build a random matrix
	//--------------------------------------------------------------------------

	GrB_Matrix A;
	GrB_Index nvals = 0;
	GrB_Index nrows = 1024;
	GrB_Index ncols = 1024;
	GrB_Index *I = new GrB_Index[ncols * nrows];
	GrB_Index *J = new GrB_Index[ncols * nrows];
	bool *X = new bool[ncols * nrows];

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

	GrB_Index *I_iter = new GrB_Index[nvals], *J_iter = new GrB_Index[nvals];

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
                I_iter[i] = row;
                J_iter[i] = col;
	}
	GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
	ASSERT_TRUE(depleted);

        bool out = check_eq_bool (A, nrows, ncols, I_iter, J_iter, nvals);
        ASSERT_TRUE(out);

	//--------------------------------------------------------------------------
	// Clean up.
	//--------------------------------------------------------------------------
	delete[] I;
	delete[] J;
	delete[] X;
        delete[] I_iter;
        delete[] J_iter;
	GxB_MatrixTupleIter_free(&iter);
	GrB_Matrix_free(&A);
}

TEST_F(TuplesTest, MatrixIteratorTest) {
	//--------------------------------------------------------------------------
	// Build a 4X4 matrix
	//--------------------------------------------------------------------------

	GrB_Index nvals = 4;
	GrB_Matrix A = CreateSquareNByNDiagonalMatrix(nvals);

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
        GrB_Index sum = 0;
	for(int i = 0; i < nvals; i++) {
		GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
		ASSERT_FALSE(depleted);
		ASSERT_EQ(row, col);
                sum += row+1;
	}
	GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
	ASSERT_TRUE(depleted);
        ASSERT_EQ((nvals*(nvals+1))/2, sum);

	//--------------------------------------------------------------------------
	// Reset iterator an re-verify.
	//--------------------------------------------------------------------------

	GxB_MatrixTupleIter_reset(iter);
        depleted = false;
        sum = 0;
	for(int i = 0; i < nvals; i++) {
		GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
		ASSERT_FALSE(depleted);
		ASSERT_EQ(row, col);
                sum += row+1;
	}
	GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
	ASSERT_TRUE(depleted);
        ASSERT_EQ((nvals*(nvals+1))/2, sum);

	//--------------------------------------------------------------------------
	// Clean up.
	//--------------------------------------------------------------------------
	GxB_MatrixTupleIter_free(&iter);
	GrB_Matrix_free(&A);
}

TEST_F(TuplesTest, RowIteratorTest) {
	//--------------------------------------------------------------------------
	// Build a 4X4 diagonal matrix
	//--------------------------------------------------------------------------

	GrB_Index nvals = 4;
	GrB_Matrix A = CreateSquareNByNDiagonalMatrix(nvals);
	GrB_Index row;
	GrB_Index col;
	GrB_Index nrows = nvals;
	GxB_MatrixTupleIter iter;
	GxB_MatrixTupleIter_new(&iter, A);

	for(int j = 0; j < nrows; j++) {
		//--------------------------------------------------------------------------
		// Test iterating over each row twice, this is to check
		// iterator reusability.
		//--------------------------------------------------------------------------

		int reuse = 2;
		for(int k = 0; k < reuse; k++) {
			//--------------------------------------------------------------------------
			// Get an iterator over the current row.
			//--------------------------------------------------------------------------
			GxB_MatrixTupleIter_iterate_row(iter, j);

			//--------------------------------------------------------------------------
			// Verify iterator returned values.
			//--------------------------------------------------------------------------
			bool depleted = false;
                        GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
                        ASSERT_FALSE(depleted);
                        ASSERT_EQ(row, j);
                        ASSERT_EQ(col, j);

			GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
			ASSERT_TRUE(depleted);
		}
	}
	GxB_MatrixTupleIter_free(&iter);
	GrB_Matrix_free(&A);
}

TEST_F(TuplesTest, RowIteratorEmptyMatrixTest) {
	//--------------------------------------------------------------------------
	// Build a 4X4 empty matrix
	//--------------------------------------------------------------------------

	GrB_Index nvals = 4;
	GrB_Matrix A = CreateSquareNByNEmptyMatrix(nvals);
	GrB_Index row;
	GrB_Index col;
	GrB_Index nrows = nvals;
	GxB_MatrixTupleIter iter;
	GxB_MatrixTupleIter_new(&iter, A);

	for(int j = 0; j < nrows; j++) {

		//--------------------------------------------------------------------------
		// Get an iterator over the current row.
		//--------------------------------------------------------------------------
		GxB_MatrixTupleIter_iterate_row(iter, j);

		//--------------------------------------------------------------------------
		// Verify iterator returned values.
		//--------------------------------------------------------------------------
		bool depleted = false;
		GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
		ASSERT_TRUE(depleted);
	}

	GxB_MatrixTupleIter_free(&iter);
	GrB_Matrix_free(&A);
}

TEST_F(TuplesTest, IteratorRange) {

	// Matrix is 6X6 and will be populated with the following indices.
        const GrB_Index nvals = 6;
        GrB_Index row_indices[nvals] = { 0, 2, 2, 3, 3, 5 };
        GrB_Index col_indices[nvals] = { 2, 1, 3, 0, 4, 5 };
        bool X[nvals];
        std::fill_n (X, nvals, true);

	bool depleted;
	GrB_Info info;
	GrB_Index row;
	GrB_Index col;

	// Create and populate the matrix.
	const GrB_Index n = 6;
	GrB_Matrix A = CreateSquareNByNEmptyMatrix(n);
	GrB_Matrix_build_BOOL(A, row_indices, col_indices, X, nvals, GrB_FIRST_BOOL);

        GrB_Vector v;
        GrB_Vector_new (&v, GrB_BOOL, n);
        GrB_Index row_iter[n], col_iter[n];
        bool val_iter[n];
        std::fill_n(val_iter, n, true);

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
	ASSERT_EQ(GrB_INDEX_OUT_OF_BOUNDS, info);

	// Iterate single row.
	info = GxB_MatrixTupleIter_iterate_range(iter, 2, 2);
	ASSERT_EQ(GrB_SUCCESS, info);

	// Check that the correct indices are retrived.
        int k = 0;
	for(int i = 1; i <= 2; i++) {
		info = GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
		ASSERT_EQ(GrB_SUCCESS, info);
                ASSERT_EQ(row, 2);
                col_iter[k++] = col;
		ASSERT_FALSE(depleted);
	}
	GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
	ASSERT_TRUE(depleted);
        ASSERT_TRUE(k == 2);

        GrB_Col_extract (v, GrB_NULL, GrB_NULL, A, GrB_ALL, n, 2, GrB_DESC_T0);
        bool out;
        out = check_eq_bool (v, n, col_iter, k);
        ASSERT_TRUE(out);
        GrB_Vector_free (&v);

	// Check for legal range setting.
	info = GxB_MatrixTupleIter_iterate_range(iter, 2, 3);
	ASSERT_EQ(GrB_SUCCESS, info);

	// Check that the correct indices are retrived.
        k = 0;
        depleted = false;
	for(int i = 1; i <= 4; i++) {
		info = GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
		ASSERT_EQ(GrB_SUCCESS, info);
                row_iter[k] = row;
                col_iter[k++] = col;
		ASSERT_FALSE(depleted);
	}
	GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
	ASSERT_TRUE(depleted);

        GrB_Index selected_rows[] = {2, 3};
        GrB_Matrix Zselected;
        GrB_Matrix_new (&Zselected, GrB_BOOL, 2, n);
        GrB_Matrix_extract (Zselected, GrB_NULL, GrB_NULL, A, selected_rows, 2, GrB_ALL, n, GrB_NULL);
        GrB_Matrix Z;
        GrB_Matrix_new (&Z, GrB_BOOL, n, n);
        GrB_Matrix_assign (Z, GrB_NULL, GrB_NULL, Zselected, selected_rows, 2, GrB_ALL, n, GrB_NULL);
        GrB_Matrix_free (&Zselected);

        out = check_eq_bool (Z, n, n, row_iter, col_iter, k);
        ASSERT_TRUE(out);
        GrB_Matrix_free (&Z);

	// Set the entire rows as range, check that iterator is depleted only when it is done iterating the matrix.
	info = GxB_MatrixTupleIter_iterate_range(iter, 0, n - 1);
	ASSERT_EQ(GrB_SUCCESS, info);

        k = 0;
	for(int i = 0; i < n; i ++) {
		info = GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
		ASSERT_EQ(GrB_SUCCESS, info);
                row_iter[k] = row;
                col_iter[k++] = col;
		ASSERT_FALSE(depleted);
	}
	GxB_MatrixTupleIter_next(iter, &row, &col, &depleted);
	ASSERT_TRUE(depleted);

        out = check_eq_bool (A, n, n, row_iter, col_iter, k);
        ASSERT_TRUE(out);

	GxB_MatrixTupleIter_free(&iter);
        GrB_Matrix_free (&A);
}

