using Sharpen;

namespace Org.Apache.Hadoop.Examples.Dancing
{
	/// <summary>Of the "normal" 12 pentominos, 6 of them have distinct shapes when flipped.
	/// 	</summary>
	/// <remarks>
	/// Of the "normal" 12 pentominos, 6 of them have distinct shapes when flipped.
	/// This class includes both variants of the "flippable" shapes and the
	/// unflippable shapes for a total of 18 pieces. Clearly, the boards must have
	/// 18*5=90 boxes to hold all of the solutions.
	/// </remarks>
	public class OneSidedPentomino : Pentomino
	{
		public OneSidedPentomino()
		{
		}

		public OneSidedPentomino(int width, int height)
			: base(width, height)
		{
		}

		/// <summary>Define the one sided pieces.</summary>
		/// <remarks>
		/// Define the one sided pieces. The flipped pieces have the same name with
		/// a capital letter.
		/// </remarks>
		protected internal override void InitializePieces()
		{
			pieces.AddItem(new Pentomino.Piece("x", " x /xxx/ x ", false, oneRotation));
			pieces.AddItem(new Pentomino.Piece("v", "x  /x  /xxx", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("t", "xxx/ x / x ", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("w", "  x/ xx/xx ", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("u", "x x/xxx", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("i", "xxxxx", false, twoRotations));
			pieces.AddItem(new Pentomino.Piece("f", " xx/xx / x ", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("p", "xx/xx/x ", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("z", "xx / x / xx", false, twoRotations));
			pieces.AddItem(new Pentomino.Piece("n", "xx  / xxx", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("y", "  x /xxxx", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("l", "   x/xxxx", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("F", "xx / xx/ x ", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("P", "xx/xx/ x", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("Z", " xx/ x /xx ", false, twoRotations));
			pieces.AddItem(new Pentomino.Piece("N", "  xx/xxx ", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("Y", " x  /xxxx", false, fourRotations));
			pieces.AddItem(new Pentomino.Piece("L", "x   /xxxx", false, fourRotations));
		}

		/// <summary>Solve the 3x30 puzzle.</summary>
		/// <param name="args"/>
		public static void Main(string[] args)
		{
			Pentomino model = new Org.Apache.Hadoop.Examples.Dancing.OneSidedPentomino(3, 30);
			int solutions = model.Solve();
			System.Console.Out.WriteLine(solutions + " solutions found.");
		}
	}
}
