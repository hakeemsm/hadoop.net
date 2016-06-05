using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.Dancing
{
	/// <summary>
	/// This class uses the dancing links algorithm from Knuth to solve sudoku
	/// puzzles.
	/// </summary>
	/// <remarks>
	/// This class uses the dancing links algorithm from Knuth to solve sudoku
	/// puzzles. It has solved 42x42 puzzles in 1.02 seconds.
	/// </remarks>
	public class Sudoku
	{
		/// <summary>
		/// The preset values in the board
		/// board[y][x] is the value at x,y with -1 = any
		/// </summary>
		private int[][] board;

		/// <summary>The size of the board</summary>
		private int size;

		/// <summary>The size of the sub-squares in cells across</summary>
		private int squareXSize;

		/// <summary>The size of the sub-squares in celss up and down</summary>
		private int squareYSize;

		/// <summary>
		/// This interface is a marker class for the columns created for the
		/// Sudoku solver.
		/// </summary>
		protected internal interface ColumnName
		{
			// NOTHING
		}

		/// <summary>A string containing a representation of the solution.</summary>
		/// <param name="size">the size of the board</param>
		/// <param name="solution">a list of list of column names</param>
		/// <returns>a string of the solution matrix</returns>
		internal static string StringifySolution(int size, IList<IList<Sudoku.ColumnName>
			> solution)
		{
			//int[][] picture = new int[size][size];
			//HM: Line above replaced with the one below due to the 2nd dim array
			int[][] picture = new int[size][];
			StringBuilder result = new StringBuilder();
			// go through the rows selected in the model and build a picture of the
			// solution.
			foreach (IList<Sudoku.ColumnName> row in solution)
			{
				int x = -1;
				int y = -1;
				int num = -1;
				foreach (Sudoku.ColumnName item in row)
				{
					if (item is Sudoku.ColumnConstraint)
					{
						x = ((Sudoku.ColumnConstraint)item).column;
						num = ((Sudoku.ColumnConstraint)item).num;
					}
					else
					{
						if (item is Sudoku.RowConstraint)
						{
							y = ((Sudoku.RowConstraint)item).row;
						}
					}
				}
				picture[y][x] = num;
			}
			// build the string
			for (int y_1 = 0; y_1 < size; ++y_1)
			{
				for (int x = 0; x < size; ++x)
				{
					result.Append(picture[y_1][x]);
					result.Append(" ");
				}
				result.Append("\n");
			}
			return result.ToString();
		}

		/// <summary>
		/// An acceptor to get the solutions to the puzzle as they are generated and
		/// print them to the console.
		/// </summary>
		private class SolutionPrinter : DancingLinks.SolutionAcceptor<Sudoku.ColumnName>
		{
			internal int size;

			public SolutionPrinter(int size)
			{
				this.size = size;
			}

			/// <summary>
			/// A debugging aid that just prints the raw information about the
			/// dancing link columns that were selected for each row.
			/// </summary>
			/// <param name="solution">a list of list of column names</param>
			internal virtual void RawWrite(IList solution)
			{
				for (IEnumerator itr = solution.GetEnumerator(); itr.HasNext(); )
				{
					IEnumerator subitr = ((IList)itr.Next()).GetEnumerator();
					while (subitr.HasNext())
					{
						System.Console.Out.Write(subitr.Next().ToString() + " ");
					}
					System.Console.Out.WriteLine();
				}
			}

			public virtual void Solution(IList<IList<Sudoku.ColumnName>> names)
			{
				System.Console.Out.WriteLine(StringifySolution(size, names));
			}
		}

		/// <summary>Set up a puzzle board to the given size.</summary>
		/// <remarks>
		/// Set up a puzzle board to the given size.
		/// Boards may be asymmetric, but the squares will always be divided to be
		/// more cells wide than they are tall. For example, a 6x6 puzzle will make
		/// sub-squares that are 3x2 (3 cells wide, 2 cells tall). Clearly that means
		/// the board is made up of 2x3 sub-squares.
		/// </remarks>
		/// <param name="stream">The input stream to read the data from</param>
		/// <exception cref="System.IO.IOException"/>
		public Sudoku(InputStream stream)
		{
			BufferedReader file = new BufferedReader(new InputStreamReader(stream, Charsets.Utf8
				));
			string line = file.ReadLine();
			IList<int[]> result = new AList<int[]>();
			while (line != null)
			{
				StringTokenizer tokenizer = new StringTokenizer(line);
				int size = tokenizer.CountTokens();
				int[] col = new int[size];
				int y = 0;
				while (tokenizer.MoveNext())
				{
					string word = tokenizer.NextToken();
					if ("?".Equals(word))
					{
						col[y] = -1;
					}
					else
					{
						col[y] = System.Convert.ToInt32(word);
					}
					y += 1;
				}
				result.AddItem(col);
				line = file.ReadLine();
			}
			size = result.Count;
			board = Sharpen.Collections.ToArray(result, new int[size][]);
			squareYSize = (int)Math.Sqrt(size);
			squareXSize = size / squareYSize;
			file.Close();
		}

		/// <summary>A constraint that each number can appear just once in a column.</summary>
		private class ColumnConstraint : Sudoku.ColumnName
		{
			internal ColumnConstraint(int num, int column)
			{
				this.num = num;
				this.column = column;
			}

			internal int num;

			internal int column;

			public override string ToString()
			{
				return num + " in column " + column;
			}
		}

		/// <summary>A constraint that each number can appear just once in a row.</summary>
		private class RowConstraint : Sudoku.ColumnName
		{
			internal RowConstraint(int num, int row)
			{
				this.num = num;
				this.row = row;
			}

			internal int num;

			internal int row;

			public override string ToString()
			{
				return num + " in row " + row;
			}
		}

		/// <summary>A constraint that each number can appear just once in a square.</summary>
		private class SquareConstraint : Sudoku.ColumnName
		{
			internal SquareConstraint(int num, int x, int y)
			{
				this.num = num;
				this.x = x;
				this.y = y;
			}

			internal int num;

			internal int x;

			internal int y;

			public override string ToString()
			{
				return num + " in square " + x + "," + y;
			}
		}

		/// <summary>A constraint that each cell can only be used once.</summary>
		private class CellConstraint : Sudoku.ColumnName
		{
			internal CellConstraint(int x, int y)
			{
				this.x = x;
				this.y = y;
			}

			internal int x;

			internal int y;

			public override string ToString()
			{
				return "cell " + x + "," + y;
			}
		}

		/// <summary>Create a row that places num in cell x, y.</summary>
		/// <param name="rowValues">a scratch pad to mark the bits needed</param>
		/// <param name="x">the horizontal offset of the cell</param>
		/// <param name="y">the vertical offset of the cell</param>
		/// <param name="num">the number to place</param>
		/// <returns>a bitvector of the columns selected</returns>
		private bool[] GenerateRow(bool[] rowValues, int x, int y, int num)
		{
			// clear the scratch array
			for (int i = 0; i < rowValues.Length; ++i)
			{
				rowValues[i] = false;
			}
			// find the square coordinates
			int xBox = x / squareXSize;
			int yBox = y / squareYSize;
			// mark the column
			rowValues[x * size + num - 1] = true;
			// mark the row
			rowValues[size * size + y * size + num - 1] = true;
			// mark the square
			rowValues[2 * size * size + (xBox * squareXSize + yBox) * size + num - 1] = true;
			// mark the cell
			rowValues[3 * size * size + size * x + y] = true;
			return rowValues;
		}

		private DancingLinks<Sudoku.ColumnName> MakeModel()
		{
			DancingLinks<Sudoku.ColumnName> model = new DancingLinks<Sudoku.ColumnName>();
			// create all of the columns constraints
			for (int x = 0; x < size; ++x)
			{
				for (int num = 1; num <= size; ++num)
				{
					model.AddColumn(new Sudoku.ColumnConstraint(num, x));
				}
			}
			// create all of the row constraints
			for (int y = 0; y < size; ++y)
			{
				for (int num = 1; num <= size; ++num)
				{
					model.AddColumn(new Sudoku.RowConstraint(num, y));
				}
			}
			// create the square constraints
			for (int x_1 = 0; x_1 < squareYSize; ++x_1)
			{
				for (int y_1 = 0; y_1 < squareXSize; ++y_1)
				{
					for (int num = 1; num <= size; ++num)
					{
						model.AddColumn(new Sudoku.SquareConstraint(num, x_1, y_1));
					}
				}
			}
			// create the cell constraints
			for (int x_2 = 0; x_2 < size; ++x_2)
			{
				for (int y_1 = 0; y_1 < size; ++y_1)
				{
					model.AddColumn(new Sudoku.CellConstraint(x_2, y_1));
				}
			}
			bool[] rowValues = new bool[size * size * 4];
			for (int x_3 = 0; x_3 < size; ++x_3)
			{
				for (int y_1 = 0; y_1 < size; ++y_1)
				{
					if (board[y_1][x_3] == -1)
					{
						// try each possible value in the cell
						for (int num = 1; num <= size; ++num)
						{
							model.AddRow(GenerateRow(rowValues, x_3, y_1, num));
						}
					}
					else
					{
						// put the given cell in place
						model.AddRow(GenerateRow(rowValues, x_3, y_1, board[y_1][x_3]));
					}
				}
			}
			return model;
		}

		public virtual void Solve()
		{
			DancingLinks<Sudoku.ColumnName> model = MakeModel();
			int results = model.Solve(new Sudoku.SolutionPrinter(size));
			System.Console.Out.WriteLine("Found " + results + " solutions");
		}

		/// <summary>Solves a set of sudoku puzzles.</summary>
		/// <param name="args">a list of puzzle filenames to solve</param>
		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			if (args.Length == 0)
			{
				System.Console.Out.WriteLine("Include a puzzle on the command line.");
			}
			for (int i = 0; i < args.Length; ++i)
			{
				Sudoku problem = new Sudoku(new FileInputStream(args[i]));
				System.Console.Out.WriteLine("Solving " + args[i]);
				problem.Solve();
			}
		}
	}
}
