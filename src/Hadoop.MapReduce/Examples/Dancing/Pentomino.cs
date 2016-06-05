using System.Collections;
using System.Collections.Generic;
using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.Dancing
{
	public class Pentomino
	{
		public const string Depth = "mapreduce.pentomino.depth";

		public const string Width = "mapreduce.pentomino.width";

		public const string Height = "mapreduce.pentomino.height";

		public const string Class = "mapreduce.pentomino.class";

		/// <summary>
		/// This interface just is a marker for what types I expect to get back
		/// as column names.
		/// </summary>
		protected internal interface ColumnName
		{
			// NOTHING
		}

		/// <summary>Maintain information about a puzzle piece.</summary>
		protected internal class Piece : Pentomino.ColumnName
		{
			private string name;

			private bool[][] shape;

			private int[] rotations;

			private bool flippable;

			public Piece(string name, string shape, bool flippable, int[] rotations)
			{
				this.name = name;
				this.rotations = rotations;
				this.flippable = flippable;
				StringTokenizer parser = new StringTokenizer(shape, "/");
				IList<bool[]> lines = new AList<bool[]>();
				while (parser.HasMoreTokens())
				{
					string token = parser.NextToken();
					bool[] line = new bool[token.Length];
					for (int i = 0; i < line.Length; ++i)
					{
						line[i] = token[i] == 'x';
					}
					lines.AddItem(line);
				}
				this.shape = new bool[lines.Count][];
				for (int i_1 = 0; i_1 < lines.Count; i_1++)
				{
					this.shape[i_1] = lines[i_1];
				}
			}

			public virtual string GetName()
			{
				return name;
			}

			public virtual int[] GetRotations()
			{
				return rotations.MemberwiseClone();
			}

			public virtual bool GetFlippable()
			{
				return flippable;
			}

			private int DoFlip(bool flip, int x, int max)
			{
				if (flip)
				{
					return max - x - 1;
				}
				else
				{
					return x;
				}
			}

			public virtual bool[][] GetShape(bool flip, int rotate)
			{
				bool[][] result;
				if (rotate % 2 == 0)
				{
					int height = shape.Length;
					int width = shape[0].Length;
					result = new bool[height][];
					bool flipX = rotate == 2;
					bool flipY = flip ^ (rotate == 2);
					for (int y = 0; y < height; ++y)
					{
						result[y] = new bool[width];
						for (int x = 0; x < width; ++x)
						{
							result[y][x] = shape[DoFlip(flipY, y, height)][DoFlip(flipX, x, width)];
						}
					}
				}
				else
				{
					int height = shape[0].Length;
					int width = shape.Length;
					result = new bool[height][];
					bool flipX = rotate == 3;
					bool flipY = flip ^ (rotate == 1);
					for (int y = 0; y < height; ++y)
					{
						result[y] = new bool[width];
						for (int x = 0; x < width; ++x)
						{
							result[y][x] = shape[DoFlip(flipX, x, width)][DoFlip(flipY, y, height)];
						}
					}
				}
				return result;
			}
		}

		/// <summary>A point in the puzzle board.</summary>
		/// <remarks>
		/// A point in the puzzle board. This represents a placement of a piece into
		/// a given point on the board.
		/// </remarks>
		internal class Point : Pentomino.ColumnName
		{
			internal int x;

			internal int y;

			internal Point(int x, int y)
			{
				this.x = x;
				this.y = y;
			}
		}

		/// <summary>
		/// Convert a solution to the puzzle returned by the model into a string
		/// that represents the placement of the pieces onto the board.
		/// </summary>
		/// <param name="width">the width of the puzzle board</param>
		/// <param name="height">the height of the puzzle board</param>
		/// <param name="solution">the list of column names that were selected in the model</param>
		/// <returns>a string representation of completed puzzle board</returns>
		public static string StringifySolution(int width, int height, IList<IList<Pentomino.ColumnName
			>> solution)
		{
			//String[][] picture = new String[height][width];
			//HM: Line above replaced with the one below due to the 2nd dim array
			string[][] picture = new string[height][];
			StringBuilder result = new StringBuilder();
			// for each piece placement...
			foreach (IList<Pentomino.ColumnName> row in solution)
			{
				// go through to find which piece was placed
				Pentomino.Piece piece = null;
				foreach (Pentomino.ColumnName item in row)
				{
					if (item is Pentomino.Piece)
					{
						piece = (Pentomino.Piece)item;
						break;
					}
				}
				// for each point where the piece was placed, mark it with the piece name
				foreach (Pentomino.ColumnName item_1 in row)
				{
					if (item_1 is Pentomino.Point)
					{
						Pentomino.Point p = (Pentomino.Point)item_1;
						picture[p.y][p.x] = piece.GetName();
					}
				}
			}
			// put the string together
			for (int y = 0; y < picture.Length; ++y)
			{
				for (int x = 0; x < picture[y].Length; ++x)
				{
					result.Append(picture[y][x]);
				}
				result.Append("\n");
			}
			return result.ToString();
		}

		public enum SolutionCategory
		{
			UpperLeft,
			MidX,
			MidY,
			Center
		}

		/// <summary>
		/// Find whether the solution has the x in the upper left quadrant, the
		/// x-midline, the y-midline or in the center.
		/// </summary>
		/// <param name="names">the solution to check</param>
		/// <returns>the catagory of the solution</returns>
		public virtual Pentomino.SolutionCategory GetCategory(IList<IList<Pentomino.ColumnName
			>> names)
		{
			Pentomino.Piece xPiece = null;
			// find the "x" piece
			foreach (Pentomino.Piece p in pieces)
			{
				if ("x".Equals(p.name))
				{
					xPiece = p;
					break;
				}
			}
			// find the row containing the "x"
			foreach (IList<Pentomino.ColumnName> row in names)
			{
				if (row.Contains(xPiece))
				{
					// figure out where the "x" is located
					int low_x = width;
					int high_x = 0;
					int low_y = height;
					int high_y = 0;
					foreach (Pentomino.ColumnName col in row)
					{
						if (col is Pentomino.Point)
						{
							int x = ((Pentomino.Point)col).x;
							int y = ((Pentomino.Point)col).y;
							if (x < low_x)
							{
								low_x = x;
							}
							if (x > high_x)
							{
								high_x = x;
							}
							if (y < low_y)
							{
								low_y = y;
							}
							if (y > high_y)
							{
								high_y = y;
							}
						}
					}
					bool mid_x = (low_x + high_x == width - 1);
					bool mid_y = (low_y + high_y == height - 1);
					if (mid_x && mid_y)
					{
						return Pentomino.SolutionCategory.Center;
					}
					else
					{
						if (mid_x)
						{
							return Pentomino.SolutionCategory.MidX;
						}
						else
						{
							if (mid_y)
							{
								return Pentomino.SolutionCategory.MidY;
							}
						}
					}
					break;
				}
			}
			return Pentomino.SolutionCategory.UpperLeft;
		}

		/// <summary>A solution printer that just writes the solution to stdout.</summary>
		private class SolutionPrinter : DancingLinks.SolutionAcceptor<Pentomino.ColumnName
			>
		{
			internal int width;

			internal int height;

			public SolutionPrinter(int width, int height)
			{
				this.width = width;
				this.height = height;
			}

			public virtual void Solution(IList<IList<Pentomino.ColumnName>> names)
			{
				System.Console.Out.WriteLine(StringifySolution(width, height, names));
			}
		}

		protected internal int width;

		protected internal int height;

		protected internal IList<Pentomino.Piece> pieces = new AList<Pentomino.Piece>();

		/// <summary>Is the piece fixed under rotation?</summary>
		protected internal static readonly int[] oneRotation = new int[] { 0 };

		/// <summary>Is the piece identical if rotated 180 degrees?</summary>
		protected internal static readonly int[] twoRotations = new int[] { 0, 1 };

		/// <summary>Are all 4 rotations unique?</summary>
		protected internal static readonly int[] fourRotations = new int[] { 0, 1, 2, 3 };

		/// <summary>Fill in the pieces list.</summary>
		protected internal virtual void InitializePieces()
		{
			pieces.AddItem(new Pentomino.Piece("x", " x /xxx/ x ", false, oneRotation));
			pieces.AddItem(new Pentomino.Piece("v", "x  /x  /xxx", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("t", "xxx/ x / x ", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("w", "  x/ xx/xx ", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("u", "x x/xxx", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("i", "xxxxx", false, twoRotations));
			pieces.AddItem(new Pentomino.Piece("f", " xx/xx / x ", true, fourRotations));
			pieces.AddItem(new Pentomino.Piece("p", "xx/xx/x ", true, fourRotations));
			pieces.AddItem(new Pentomino.Piece("z", "xx / x / xx", true, twoRotations));
			pieces.AddItem(new Pentomino.Piece("n", "xx  / xxx", true, fourRotations));
			pieces.AddItem(new Pentomino.Piece("y", "  x /xxxx", true, fourRotations));
			pieces.AddItem(new Pentomino.Piece("l", "   x/xxxx", true, fourRotations));
		}

		/// <summary>
		/// Is the middle of piece on the upper/left side of the board with
		/// a given offset and size of the piece? This only checks in one
		/// dimension.
		/// </summary>
		/// <param name="offset">the offset of the piece</param>
		/// <param name="shapeSize">the size of the piece</param>
		/// <param name="board">the size of the board</param>
		/// <returns>is it in the upper/left?</returns>
		private static bool IsSide(int offset, int shapeSize, int board)
		{
			return 2 * offset + shapeSize <= board;
		}

		/// <summary>
		/// For a given piece, generate all of the potential placements and add them
		/// as rows to the model.
		/// </summary>
		/// <param name="dancer">the problem model</param>
		/// <param name="piece">the piece we are trying to place</param>
		/// <param name="width">the width of the board</param>
		/// <param name="height">the height of the board</param>
		/// <param name="flip">is the piece flipped over?</param>
		/// <param name="row">a workspace the length of the each row in the table</param>
		/// <param name="upperLeft">
		/// is the piece constrained to the upper left of the board?
		/// this is used on a single piece to eliminate most of the trivial
		/// roations of the solution.
		/// </param>
		private static void GenerateRows(DancingLinks dancer, Pentomino.Piece piece, int 
			width, int height, bool flip, bool[] row, bool upperLeft)
		{
			// for each rotation
			int[] rotations = piece.GetRotations();
			for (int rotIndex = 0; rotIndex < rotations.Length; ++rotIndex)
			{
				// get the shape
				bool[][] shape = piece.GetShape(flip, rotations[rotIndex]);
				// find all of the valid offsets
				for (int x = 0; x < width; ++x)
				{
					for (int y = 0; y < height; ++y)
					{
						if (y + shape.Length <= height && x + shape[0].Length <= width && (!upperLeft || 
							(IsSide(x, shape[0].Length, width) && IsSide(y, shape.Length, height))))
						{
							// clear the columns related to the points on the board
							for (int idx = 0; idx < width * height; ++idx)
							{
								row[idx] = false;
							}
							// mark the shape
							for (int subY = 0; subY < shape.Length; ++subY)
							{
								for (int subX = 0; subX < shape[0].Length; ++subX)
								{
									row[(y + subY) * width + x + subX] = shape[subY][subX];
								}
							}
							dancer.AddRow(row);
						}
					}
				}
			}
		}

		private DancingLinks<Pentomino.ColumnName> dancer = new DancingLinks<Pentomino.ColumnName
			>();

		private DancingLinks.SolutionAcceptor<Pentomino.ColumnName> printer;

		/// <summary>Create the model for a given pentomino set of pieces and board size.</summary>
		/// <param name="width">the width of the board in squares</param>
		/// <param name="height">the height of the board in squares</param>
		public Pentomino(int width, int height)
		{
			{
				InitializePieces();
			}
			Initialize(width, height);
		}

		/// <summary>Create the object without initialization.</summary>
		public Pentomino()
		{
			{
				InitializePieces();
			}
		}

		internal virtual void Initialize(int width, int height)
		{
			this.width = width;
			this.height = height;
			for (int y = 0; y < height; ++y)
			{
				for (int x = 0; x < width; ++x)
				{
					dancer.AddColumn(new Pentomino.Point(x, y));
				}
			}
			int pieceBase = dancer.GetNumberColumns();
			foreach (Pentomino.Piece p in pieces)
			{
				dancer.AddColumn(p);
			}
			bool[] row = new bool[dancer.GetNumberColumns()];
			for (int idx = 0; idx < pieces.Count; ++idx)
			{
				Pentomino.Piece piece = pieces[idx];
				row[idx + pieceBase] = true;
				GenerateRows(dancer, piece, width, height, false, row, idx == 0);
				if (piece.GetFlippable())
				{
					GenerateRows(dancer, piece, width, height, true, row, idx == 0);
				}
				row[idx + pieceBase] = false;
			}
			printer = new Pentomino.SolutionPrinter(width, height);
		}

		/// <summary>Generate a list of prefixes to a given depth</summary>
		/// <param name="depth">the length of each prefix</param>
		/// <returns>a list of arrays of ints, which are potential prefixes</returns>
		public virtual IList<int[]> GetSplits(int depth)
		{
			return dancer.Split(depth);
		}

		/// <summary>Find all of the solutions that start with the given prefix.</summary>
		/// <remarks>
		/// Find all of the solutions that start with the given prefix. The printer
		/// is given each solution as it is found.
		/// </remarks>
		/// <param name="split">
		/// a list of row indexes that should be choosen for each row
		/// in order
		/// </param>
		/// <returns>the number of solutions found</returns>
		public virtual int Solve(int[] split)
		{
			return dancer.Solve(split, printer);
		}

		/// <summary>Find all of the solutions to the puzzle.</summary>
		/// <returns>the number of solutions found</returns>
		public virtual int Solve()
		{
			return dancer.Solve(printer);
		}

		/// <summary>Set the printer for the puzzle.</summary>
		/// <param name="printer">
		/// A call-back object that is given each solution as it is
		/// found.
		/// </param>
		public virtual void SetPrinter(DancingLinks.SolutionAcceptor<Pentomino.ColumnName
			> printer)
		{
			this.printer = printer;
		}

		/// <summary>Solve the 6x10 pentomino puzzle.</summary>
		public static void Main(string[] args)
		{
			int width = 6;
			int height = 10;
			Pentomino model = new Pentomino(width, height);
			IList splits = model.GetSplits(2);
			for (IEnumerator splitItr = splits.GetEnumerator(); splitItr.HasNext(); )
			{
				int[] choices = (int[])splitItr.Next();
				System.Console.Out.Write("split:");
				for (int i = 0; i < choices.Length; ++i)
				{
					System.Console.Out.Write(" " + choices[i]);
				}
				System.Console.Out.WriteLine();
				System.Console.Out.WriteLine(model.Solve(choices) + " solutions found.");
			}
		}
	}
}
