package coflow.model;

import java.util.ArrayList;
import java.util.List;

public class BvN {
	
	public final int multiples = 100000000;
	
	public long RHO;
	
	public double maximumRowAndColumuSums;

	private List<List<Integer>> adjacent = null;

	private int[] matching = null;

	private boolean[] check = null;

	public List<int[][]> A = null;

	private List<int[][]> P = null;

	private List<Integer> alpha = null;
	
	public List<int[][]> correctedP = null;
	
	public List<Integer> correctedAlpha = null;
	
	public boolean decompose(double[][] matrix) {
		//System.out.println("!!!!!!begin decompose()!!!!!!");
		
		double[][] normalizedMatrix = normalize(matrix);
		
		int[][] mulMatrix = multiply(multiples, normalizedMatrix);
		
		int[][] augment = augmentMatrix(mulMatrix);
		
		RHO = getMaximumRowAndColumnSums(augment);
		
//		double[][] doublyStochastic = toDoublyStochasticMatrix(augment);
//		printMatrix(matrix);
//		printMatrix(mulMatrix);
//		printMatrix(augment);
//		System.out.println("eta : " + getMinimumRowAndColumnSums(augment) + " rho : " + getMaximumRowAndColumnSums(augment));
////		printMatrix(doublyStochastic);
//		System.out
//				.println("-----------------------------------------------------------------------------------------");
		birkhoff(augment);

//		int size = alpha.size();
//		for (int i = 0; i < size; i++) {
//			System.out.println();
//			System.out.println(alpha.get(i));
//			printMatrix(P.get(i));
//		}
		
//		System.out.println();

//		correct(getPrimitiveMatrix(matrix), doublyStochastic);
		correct(mulMatrix, augment);
		
//		size = correctedAlpha.size();
//		for (int i = 0; i < size; i++) {
//			System.out.println();
//			System.out.println(correctedAlpha.get(i));
//			printMatrix(correctedP.get(i));
//		}
		
		
		
//		System.out.println("***********************************************************");
//		System.out.println("*****************************test**************************");
//		System.out.println("***********************************************************");
//		System.out.println("alpha size : " + correctedAlpha.size());
//		System.out.println(correctedAlpha.toString());
//		System.out.println("P size : " + correctedP.size());
//		int n = matrix.length;
//		double[][] res = new double[n][n];
//		for (int i = 0; i < correctedP.size(); i++) {
//			res = add(res, multiply(correctedAlpha.get(i), correctedP.get(i)));
//		}
//		System.out.println("res : ");
//		printMatrix(res);

		//System.out.println("!!!!!!end decompose()!!!!!!");

		return true;
	}
	
	private double[][] normalize(double[][] matrix) {
		maximumRowAndColumuSums = getMaximumRowAndColumnSums(matrix);
		
		int m = matrix.length;
		int n = matrix[0].length;
		
		double[][] resMatrix = new double[m][n];
		
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				resMatrix[i][j] = matrix[i][j] / maximumRowAndColumuSums;
			}
		}
		
		return resMatrix;
	}
	
	// get the maximum of row sums and column sums
	private double getMaximumRowAndColumnSums(double[][] matrix) {
		int m = matrix.length;
		if (0 == m) {
			System.out
					.println("------getMaximumRowAndColumnSums() error------");
			return Long.MIN_VALUE;
		}
		int n = matrix[0].length;

		double max = getRowSums(0, matrix);

		for (int i = 0; i < m; i++) {
			double tmp = getRowSums(i, matrix);
			if (tmp > max)
				max = tmp;
		}

		for (int i = 0; i < n; i++) {
			double tmp = getColSums(i, matrix);
			if (tmp > max)
				max = tmp;
		}

		return max;
	}
	
	// get row sums
	private double getRowSums(int row, double[][] matrix) {
		int m = matrix.length;
		if (0 == m || row >= m) {
			System.out.println("------getRowSums() error------");
			return Double.MIN_VALUE;
		}
		int n = matrix[row].length;

		double sum = 0;
		for (int i = 0; i < n; i++) {
			sum += matrix[row][i];
		}
		return sum;
	}

	// get column sums
	private double getColSums(int col, double[][] matrix) {
		int m = matrix.length;
		if (0 == m || col >= m) {
			System.out.println("------getColSums() error------");
			return Double.MIN_VALUE;
		}

		double sum = 0;
		for (int i = 0; i < m; i++) {
			sum += matrix[i][col];
		}
		return sum;
	}

	// multiply matrix by a number
	private int[][] multiply(int a, double[][] matrix) {
		int m = matrix.length;
		if (0 == m) {
			System.out.println("------multiply() error------");
			return null;
		}
		int n = matrix[0].length;

		int[][] resMatrix = new int[m][n];
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				resMatrix[i][j] = (int) (matrix[i][j] * a);
			}
		}
		return resMatrix;
	}

	// correct
	private void correct(int[][] primitiveMatrix, int[][] augment) {
//		System.out.println("!!!!!!begin correct()!!!!!!");
//		System.out.println("primitiveMatrix : ");
//		printMatrix(primitiveMatrix);
//		System.out.println("doublyStochasticMatrix : ");
//		printMatrix(doublyStochasticMatrix);
		
		int m = primitiveMatrix.length;
		int n = primitiveMatrix[0].length;
		
		correctedAlpha = new ArrayList<Integer>();
		correctedP = new ArrayList<int[][]>();
		int size = alpha.size();
		for (int i = 0; i < size; i++) {
			correctedAlpha.add(alpha.get(i));
			correctedP.add(copy(P.get(i)));
		}
		
		
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				if (augment[i][j] - primitiveMatrix[i][j] == 0) {
					continue;
				}
				
				int res = 0;
				int l = correctedAlpha.size();
				
				boolean flag = false;
				int index = -1;
				int alpha1 = 0;
				int alpha2 = 0;
				for (int k = 0; k < l; k++) {
					if (flag)
						break;
					if (correctedP.get(k)[i][j] == 1) {
						res += correctedAlpha.get(k);
						if (res - primitiveMatrix[i][j] > 0) {
							flag = true;
							index = k;
							alpha2 = res - primitiveMatrix[i][j];
							alpha1 = correctedAlpha.get(k) - alpha2;
						}
					}
				}
				
//				System.out.println("i : " + i + " j : " + j);
//				System.out.println("index : " + index + " alpha1 : " + alpha1 + " alpha2 : " + alpha2);
				
				if (alpha1 == 0) {
					for (int k = index; k < correctedP.size(); k++) {
						if (correctedP.get(k)[i][j] == 1)
							correctedP.get(k)[i][j] = 0;
					}
				} else {
					correctedAlpha.set(index, alpha2);
					correctedAlpha.add(index, alpha1);
					
					int[][] tmp = correctedP.get(index);
//				int[][] tmp1 = copy(tmp);
					int[][] tmp2 = copy(tmp);
					tmp2[i][j] = 0;
					
//					System.out.println("tmp");
//					printMatrix(tmp);
//					System.out.println("tmp2");
//					printMatrix(tmp2);
					
					correctedP.add(index + 1, tmp2);
					
					for (int k = index + 2; k < correctedP.size(); k++) {
						if (correctedP.get(k)[i][j] == 1)
							correctedP.get(k)[i][j] = 0;
					}
				}
			}
		}
		
//		System.out.println("!!!!!!end correct()!!!!!!");
	}
	
	private int[][] copy(int[][] matrix) {
		int m = matrix.length;
		int n = matrix[0].length;
		
		int[][] resMatrix = new int[m][n];
		for(int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				resMatrix[i][j] = matrix[i][j];
			}
		}
		
		return resMatrix;
	}
		
	// BvN decomposition
	// Birkhoff¡¯s heuristic
	private void birkhoff(int[][] augment) {
//		System.out.println("######begin birkhoff()######");
//		System.out.println("EPS : " + EPS);

		A = new ArrayList<int[][]>();
		P = new ArrayList<int[][]>();
		alpha = new ArrayList<Integer>();

		int[][] mat = augment;
//		int count = 0;

		while (!zeroMatrix(mat)) {
//			System.out.println("\n\n\n\n\n\n");
//			System.out.println("######mat######");
//			printMatrix(mat);
//			count++;
//			System.out.println("count : " + count);
			A.add(mat);
			int[] smallestNonzeroAndIndex = getSmallestNonzeroAndIndex(mat);
			alpha.add(smallestNonzeroAndIndex[0]);
			int[] perfectMatching = getPerfectMatching(smallestNonzeroAndIndex,
					mat);
			if (null == perfectMatching) {
				System.out.println("error");
				return;
			}
			int[][] permutationMatrix = getPermutationMatrix(perfectMatching);
			P.add(permutationMatrix);
//			System.out.println("######permutationMatrix######");
//			printMatrix(permutationMatrix);
//			System.out.println("smallest : " + smallestNonzeroAndIndex[0]);
			mat = sub(mat,
					multiply(smallestNonzeroAndIndex[0], permutationMatrix));
			// eliminate floating point errors
//			for (int i = 0; i < mat.length; i++) {
//				for (int j = 0; j < mat[0].length; j++) {
//					if (mat[i][j] < EPS) {
//						mat[i][j] = 0;
//					}
//				}
//			}
		}

//		System.out.println("######end birkhoff()######");
	}
	
	// multiply matrix by a number
	private int[][] multiply(int a, int[][] matrix) {
		int m = matrix.length;
		if (0 == m) {
			System.out.println("------multiply() error------");
			return null;
		}
		int n = matrix[0].length;

		int[][] resMatrix = new int[m][n];
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				resMatrix[i][j] = matrix[i][j] * a;
			}
		}
		return resMatrix;
	}

	// matrix subtraction
	private int[][] sub(int[][] a, int[][] b) {
		int m = a.length;
		if (0 == m || m != b.length) {
			System.out.println("------sub() error------");
			return null;
		}
		int n = a[0].length;
		if (n != b[0].length) {
			System.out.println("------sub() error------");
			return null;
		}

		int[][] resMatrix = new int[m][n];
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				resMatrix[i][j] = a[i][j] - b[i][j];
			}
		}
		return resMatrix;
	}
	
	private boolean zeroMatrix(int[][] matrix) {
		int m = matrix.length;
		if (0 == m) {
			System.out.println("######zeroMatrix() error######");
			return true;
		}
		int n = matrix[0].length;

		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				// if (matrix[i][j] != 0) {
				if (matrix[i][j] != 0) {
					return false;
				}
			}
		}
		return true;
	}	

	private int[][] getPermutationMatrix(int[] perfectMatching) {
		int m = perfectMatching.length;

		int[][] resMatrix = new int[m / 2][m / 2];

		for (int i = 0; i < m / 2; i++) {
			resMatrix[i][perfectMatching[i] - m / 2] = 1;
		}

		return resMatrix;
	}
	

	// get perfect matching
	private int[] getPerfectMatching(int[] smallestNonzeroAndIndex,
			int[][] matrix) {
		int len = smallestNonzeroAndIndex.length;
		if (3 != len) {
			System.out.println("######getPerfectMatching() error######");
			return null;
		}

		int m = matrix.length;
		if (0 == m) {
			System.out.println("######getPerfectMatching() error######");
			return null;
		}
		int n = matrix[0].length;
		if (m != n) {
			System.out
					.println("######getPerfectMatching() error : not a square matrix######");
			return null;
		}

//		System.out.println("smallestNonzeroAndIndex : "
//				+ smallestNonzeroAndIndex[0] + " , "
//				+ smallestNonzeroAndIndex[1] + " , "
//				+ smallestNonzeroAndIndex[2]);

		int rowIndex = smallestNonzeroAndIndex[1];
		int colIndex = smallestNonzeroAndIndex[2];

		int[][] newMatrix = transformMatrix(rowIndex, colIndex, matrix);

//		System.out.println("------new Matrix------");
//		printMatrix(newMatrix);

		int[] perfectMatching = perfectMatching(newMatrix);

//		System.out.println("perfectMatching : "
//				+ Arrays.toString(perfectMatching));

		// perfectMatching error
		for (int i = 0; i < perfectMatching.length; i++) {
			if (perfectMatching[i] == -1) {
				System.out.println("######perfectMatching error######");
				return null;
			}
		}

		int[] res = new int[2 * m];

		for (int i = 0; i < m - 1; i++) {
			if (i < rowIndex) {
				res[i] = (perfectMatching[i] < m - 1 + colIndex ? perfectMatching[i] + 1
						: perfectMatching[i] + 2);
			} else {
				res[i + 1] = (perfectMatching[i] < m - 1 + colIndex ? perfectMatching[i] + 1
						: perfectMatching[i] + 2);
			}
		}

		res[rowIndex] = m + colIndex;

		for (int i = 0; i < m; i++) {
			res[res[i]] = i;
		}

//		System.out.println("res : " + Arrays.toString(res));

		return res;
	}
	

	// get perfect matching
	private int[] perfectMatching(int[][] matrix) {

		int n = matrix.length;

		adjacent = new ArrayList<List<Integer>>();

		for (int i = 0; i < n; i++) {
			List<Integer> adjList = new ArrayList<Integer>();
			for (int j = 0; j < n; j++) {
				if (matrix[i][j] != 0) {
					adjList.add(n + j);
				}
			}
			adjacent.add(adjList);
		}

		for (int i = 0; i < n; i++) {
			List<Integer> adjList = new ArrayList<Integer>();
			for (int j = 0; j < n; j++) {
				if (matrix[j][i] != 0) {
					adjList.add(j);
				}
			}
			adjacent.add(adjList);
		}

		matching = new int[2 * n];
		for (int i = 0; i < 2 * n; i++) {
			matching[i] = -1;
		}
		check = new boolean[2 * n];

//		int ans = 0;

		for (int i = 0; i < n; i++) {
			if (matching[i] == -1) {

				for (int j = 0; j < 2 * n; j++) {
					check[j] = false;
				}

				if (dfs(i)) {
//					ans++;
				}
			}
		}

//		System.out.println("ans : " + ans);

		return matching;
	}
	

	private boolean dfs(int u) {
		for (int i = 0; i < adjacent.get(u).size(); i++) {
			int v = adjacent.get(u).get(i);
			if (!check[v]) {
				check[v] = true;
				if (matching[v] == -1 || dfs(matching[v])) {
					matching[v] = u;
					matching[u] = v;
					return true;
				}
			}
		}
		return false;
	}
	

	// transform matrix: delete row in rowIndex and column in colIndex
	private int[][] transformMatrix(int rowIndex, int colIndex,
			int[][] matrix) {
		int n = matrix.length;
		if (n < 2) {
			System.out.println("######transformMatrix() error######");
			return null;
		}

		int[][] resMatrix = new int[n - 1][n - 1];

		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				if (i < rowIndex && j < colIndex) {
					resMatrix[i][j] = matrix[i][j];
				} else if (i > rowIndex && j < colIndex) {
					resMatrix[i - 1][j] = matrix[i][j];
				} else if (i < rowIndex && j > colIndex) {
					resMatrix[i][j - 1] = matrix[i][j];
				} else if (i > rowIndex && j > colIndex) {
					resMatrix[i - 1][j - 1] = matrix[i][j];
				} else {
					// do nothing
				}
			}
		}

		return resMatrix;
	}
	

	// double[] {min, rowIndex, colIndex}
	private int[] getSmallestNonzeroAndIndex(int[][] matrix) {
		int m = matrix.length;
		if (0 == m) {
			System.out
					.println("######getSmallestNonzeroAndIndex() error######");
			return null;
		}
		int n = matrix[0].length;
		if (0 == n) {
			System.out
					.println("######getSmallestNonzeroAndIndex() error######");
			return null;
		}

		int rowIndex = -1;
		int colIndex = -1;
		int min = Integer.MAX_VALUE;

		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				 if (matrix[i][j] < min && matrix[i][j] != 0) {
//				if (matrix[i][j] < min) {
					rowIndex = i;
					colIndex = j;
					min = matrix[i][j];
				}
			}
		}

		return new int[] { min, rowIndex, colIndex };
	}

	// BvN decomposition
	// A greedy heuristic
	// ......

	private int[][] augmentMatrix(int[][] matrix) {
		int m = matrix.length;
		if (0 == m) {
			System.out.println("------augmentMatrix() error------");
			return null;
		}
		int n = matrix[0].length;
		if (m != n) {
			System.out
					.println("------augmentMatrix() error : not square matrix------");
			return null;
		}

		int[][] resMatrix = new int[m][n];
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				resMatrix[i][j] = matrix[i][j];
			}
		}

		long rho = getMaximumRowAndColumnSums(matrix);
		long eta = getMinimumRowAndColumnSums(matrix);

		while (eta < rho) {
//			System.out.println("i");
			int i = getRowIndex(resMatrix);
			int j = getColIndex(resMatrix);

			long sumRow = getRowSums(i, resMatrix);
			long sumCol = getColSums(j, resMatrix);

			long p = (sumRow > sumCol ? (rho - sumRow) : (rho - sumCol));

			int[][] E = new int[m][n];
			for (int k = 0; k < m; k++) {
				for (int l = 0; l < n; l++) {
					if (k == i && l == j)
						E[i][j] = 1;
				}
			}

			resMatrix = add(resMatrix, multiply(p, E));
			eta = getMinimumRowAndColumnSums(resMatrix);
		}

		return resMatrix;
	}
	
	// multiply matrix by a number
	private int[][] multiply(long a, int[][] matrix) {
		int m = matrix.length;
		if (0 == m) {
			System.out.println("------multiply() error------");
			return null;
		}
		int n = matrix[0].length;

		int[][] resMatrix = new int[m][n];
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				resMatrix[i][j] = (int) (matrix[i][j] * a);
			}
		}
		return resMatrix;
	}
	
	// matrix addition
	private int[][] add(int[][] a, int[][] b) {
		int m = a.length;
		if (0 == m || m != b.length) {
			System.out.println("------add() error------");
			return null;
		}
		int n = a[0].length;
		if (n != b[0].length) {
			System.out.println("------add() error------");
			return null;
		}

		int[][] resMatrix = new int[m][n];
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				resMatrix[i][j] = a[i][j] + b[i][j];
			}
		}
		return resMatrix;
	}

	// get the index of row which have the minimum of row sums
	private int getRowIndex(int[][] matrix) {
		int m = matrix.length;
		if (0 == m) {
			System.out.println("------getRowIndex() error------");
			return Integer.MIN_VALUE;
		}

		long min = getRowSums(0, matrix);
		int index = 0;

		for (int i = 0; i < m; i++) {
			long tmp = getRowSums(i, matrix);
			if (tmp < min) {
				min = tmp;
				index = i;
			}
		}

		return index;
	}

	// get the index of column which have the minimum of column sums
	private int getColIndex(int[][] matrix) {
		int m = matrix.length;
		if (0 == m) {
			System.out.println("------getColIndex() error------");
			return Integer.MIN_VALUE;
		}
		int n = matrix[0].length;
		if (0 == n) {
			System.out.println("------getColIndex() error------");
			return Integer.MIN_VALUE;
		}

		long min = getColSums(0, matrix);
		int index = 0;

		for (int i = 0; i < n; i++) {
			long tmp = getColSums(i, matrix);
			if (tmp < min) {
				min = tmp;
				index = i;
			}
		}

		return index;
	}

	// get the minimum of row sums and column sums
	private long getMinimumRowAndColumnSums(int[][] matrix) {
		int m = matrix.length;
		if (0 == m) {
			System.out
					.println("------getMinimumRowAndColumnSums() error------");
			return Long.MIN_VALUE;
		}
		int n = matrix[0].length;

		long min = getRowSums(0, matrix);

		for (int i = 0; i < m; i++) {
			long tmp = getRowSums(i, matrix);
			if (tmp < min)
				min = tmp;
		}

		for (int i = 0; i < n; i++) {
			long tmp = getColSums(i, matrix);
			if (tmp < min)
				min = tmp;
		}

		return min;
	}

	// get the maximum of row sums and column sums
	private long getMaximumRowAndColumnSums(int[][] matrix) {
		int m = matrix.length;
		if (0 == m) {
			System.out
					.println("------getMaximumRowAndColumnSums() error------");
			return Long.MIN_VALUE;
		}
		int n = matrix[0].length;

		long max = getRowSums(0, matrix);

		for (int i = 0; i < m; i++) {
			long tmp = getRowSums(i, matrix);
			if (tmp > max)
				max = tmp;
		}

		for (int i = 0; i < n; i++) {
			long tmp = getColSums(i, matrix);
			if (tmp > max)
				max = tmp;
		}

		return max;
	}

	// get row sums
	private long getRowSums(int row, int[][] matrix) {
		int m = matrix.length;
		if (0 == m || row >= m) {
			System.out.println("------getRowSums() error------");
			return Long.MIN_VALUE;
		}
		int n = matrix[row].length;

		long sum = 0;
		for (int i = 0; i < n; i++) {
			sum += matrix[row][i];
		}
		return sum;
	}

	// get column sums
	private long getColSums(int col, int[][] matrix) {
		int m = matrix.length;
		if (0 == m || col >= m) {
			System.out.println("------getColSums() error------");
			return Long.MIN_VALUE;
		}

		long sum = 0;
		for (int i = 0; i < m; i++) {
			sum += matrix[i][col];
		}
		return sum;
	}

	// print matrix
	public void printMatrix(int[][] matrix) {
		System.out.println("------begin printMatrix()------");
		int m = matrix.length;
		if (0 == m) {
			System.out.println("------printMatrix() error------");
			return;
		}
		int n = matrix[0].length;

		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				System.out.print(matrix[i][j] + " \t");
			}
			System.out.println();
		}
		System.out.println("------end printMatrix()------");
	}

	public void printMatrix(double[][] matrix) {
		// DecimalFormat df = new DecimalFormat("#.000000000000000");
		System.out.println("------begin printMatrix()------");
		int m = matrix.length;
		if (0 == m) {
			System.out.println("------printMatrix() error------");
			return;
		}
		int n = matrix[0].length;

		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				// System.out.print(df.format(matrix[i][j]) + " \t");
				System.out.print(matrix[i][j] + " \t");
			}
			System.out.println();
		}
		System.out.println("------end printMatrix()------");
	}
}
