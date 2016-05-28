using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.Dancing
{
	/// <summary>
	/// A generic solver for tile laying problems using Knuth's dancing link
	/// algorithm.
	/// </summary>
	/// <remarks>
	/// A generic solver for tile laying problems using Knuth's dancing link
	/// algorithm. It provides a very fast backtracking data structure for problems
	/// that can expressed as a sparse boolean matrix where the goal is to select a
	/// subset of the rows such that each column has exactly 1 true in it.
	/// The application gives each column a name and each row is named after the
	/// set of columns that it has as true. Solutions are passed back by giving the
	/// selected rows' names.
	/// The type parameter ColumnName is the class of application's column names.
	/// </remarks>
	public class DancingLinks<ColumnName>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Examples.Dancing.DancingLinks
			).FullName);

		/// <summary>
		/// A cell in the table with up/down and left/right links that form doubly
		/// linked lists in both directions.
		/// </summary>
		/// <remarks>
		/// A cell in the table with up/down and left/right links that form doubly
		/// linked lists in both directions. It also includes a link to the column
		/// head.
		/// </remarks>
		private class Node<ColumnName>
		{
			internal DancingLinks.Node<ColumnName> left;

			internal DancingLinks.Node<ColumnName> right;

			internal DancingLinks.Node<ColumnName> up;

			internal DancingLinks.Node<ColumnName> down;

			internal DancingLinks.ColumnHeader<ColumnName> head;

			internal Node(DancingLinks.Node<ColumnName> l, DancingLinks.Node<ColumnName> r, DancingLinks.Node
				<ColumnName> u, DancingLinks.Node<ColumnName> d, DancingLinks.ColumnHeader<ColumnName
				> h)
			{
				left = l;
				right = r;
				up = u;
				down = d;
				head = h;
			}

			internal Node()
				: this(null, null, null, null, null)
			{
			}
		}

		/// <summary>
		/// Column headers record the name of the column and the number of rows that
		/// satisfy this column.
		/// </summary>
		/// <remarks>
		/// Column headers record the name of the column and the number of rows that
		/// satisfy this column. The names are provided by the application and can
		/// be anything. The size is used for the heuristic for picking the next
		/// column to explore.
		/// </remarks>
		private class ColumnHeader<ColumnName> : DancingLinks.Node<ColumnName>
		{
			internal ColumnName name;

			internal int size;

			internal ColumnHeader(ColumnName n, int s)
			{
				name = n;
				size = s;
				head = this;
			}

			internal ColumnHeader()
				: this(null, 0)
			{
			}
		}

		/// <summary>The head of the table.</summary>
		/// <remarks>
		/// The head of the table. Left/Right from the head are the unsatisfied
		/// ColumnHeader objects.
		/// </remarks>
		private DancingLinks.ColumnHeader<ColumnName> head;

		/// <summary>The complete list of columns.</summary>
		private IList<DancingLinks.ColumnHeader<ColumnName>> columns;

		public DancingLinks()
		{
			head = new DancingLinks.ColumnHeader<ColumnName>(null, 0);
			head.left = head;
			head.right = head;
			head.up = head;
			head.down = head;
			columns = new AList<DancingLinks.ColumnHeader<ColumnName>>(200);
		}

		/// <summary>Add a column to the table</summary>
		/// <param name="name">
		/// The name of the column, which will be returned as part of
		/// solutions
		/// </param>
		/// <param name="primary">Is the column required for a solution?</param>
		public virtual void AddColumn(ColumnName name, bool primary)
		{
			DancingLinks.ColumnHeader<ColumnName> top = new DancingLinks.ColumnHeader<ColumnName
				>(name, 0);
			top.up = top;
			top.down = top;
			if (primary)
			{
				DancingLinks.Node<ColumnName> tail = head.left;
				tail.right = top;
				top.left = tail;
				top.right = head;
				head.left = top;
			}
			else
			{
				top.left = top;
				top.right = top;
			}
			columns.AddItem(top);
		}

		/// <summary>Add a column to the table</summary>
		/// <param name="name">The name of the column, which will be included in the solution
		/// 	</param>
		public virtual void AddColumn(ColumnName name)
		{
			AddColumn(name, true);
		}

		/// <summary>Get the number of columns.</summary>
		/// <returns>the number of columns</returns>
		public virtual int GetNumberColumns()
		{
			return columns.Count;
		}

		/// <summary>Get the name of a given column as a string</summary>
		/// <param name="index">the index of the column</param>
		/// <returns>a string representation of the name</returns>
		public virtual string GetColumnName(int index)
		{
			return columns[index].name.ToString();
		}

		/// <summary>Add a row to the table.</summary>
		/// <param name="values">the columns that are satisfied by this row</param>
		public virtual void AddRow(bool[] values)
		{
			DancingLinks.Node<ColumnName> prev = null;
			for (int i = 0; i < values.Length; ++i)
			{
				if (values[i])
				{
					DancingLinks.ColumnHeader<ColumnName> top = columns[i];
					top.size += 1;
					DancingLinks.Node<ColumnName> bottom = top.up;
					DancingLinks.Node<ColumnName> node = new DancingLinks.Node<ColumnName>(null, null
						, bottom, top, top);
					bottom.down = node;
					top.up = node;
					if (prev != null)
					{
						DancingLinks.Node<ColumnName> front = prev.right;
						node.left = prev;
						node.right = front;
						prev.right = node;
						front.left = node;
					}
					else
					{
						node.left = node;
						node.right = node;
					}
					prev = node;
				}
			}
		}

		/// <summary>
		/// Applications should implement this to receive the solutions to their
		/// problems.
		/// </summary>
		public interface SolutionAcceptor<ColumnName>
		{
			/// <summary>A callback to return a solution to the application.</summary>
			/// <param name="value">
			/// a List of List of ColumnNames that were satisfied by each
			/// selected row
			/// </param>
			void Solution(IList<IList<ColumnName>> value);
		}

		/// <summary>Find the column with the fewest choices.</summary>
		/// <returns>The column header</returns>
		private DancingLinks.ColumnHeader<ColumnName> FindBestColumn()
		{
			int lowSize = int.MaxValue;
			DancingLinks.ColumnHeader<ColumnName> result = null;
			DancingLinks.ColumnHeader<ColumnName> current = (DancingLinks.ColumnHeader<ColumnName
				>)head.right;
			while (current != head)
			{
				if (current.size < lowSize)
				{
					lowSize = current.size;
					result = current;
				}
				current = (DancingLinks.ColumnHeader<ColumnName>)current.right;
			}
			return result;
		}

		/// <summary>Hide a column in the table</summary>
		/// <param name="col">the column to hide</param>
		private void CoverColumn(DancingLinks.ColumnHeader<ColumnName> col)
		{
			Log.Debug("cover " + col.head.name);
			// remove the column
			col.right.left = col.left;
			col.left.right = col.right;
			DancingLinks.Node<ColumnName> row = col.down;
			while (row != col)
			{
				DancingLinks.Node<ColumnName> node = row.right;
				while (node != row)
				{
					node.down.up = node.up;
					node.up.down = node.down;
					node.head.size -= 1;
					node = node.right;
				}
				row = row.down;
			}
		}

		/// <summary>Uncover a column that was hidden.</summary>
		/// <param name="col">the column to unhide</param>
		private void UncoverColumn(DancingLinks.ColumnHeader<ColumnName> col)
		{
			Log.Debug("uncover " + col.head.name);
			DancingLinks.Node<ColumnName> row = col.up;
			while (row != col)
			{
				DancingLinks.Node<ColumnName> node = row.left;
				while (node != row)
				{
					node.head.size += 1;
					node.down.up = node;
					node.up.down = node;
					node = node.left;
				}
				row = row.up;
			}
			col.right.left = col;
			col.left.right = col;
		}

		/// <summary>
		/// Get the name of a row by getting the list of column names that it
		/// satisfies.
		/// </summary>
		/// <param name="row">the row to make a name for</param>
		/// <returns>the list of column names</returns>
		private IList<ColumnName> GetRowName(DancingLinks.Node<ColumnName> row)
		{
			IList<ColumnName> result = new AList<ColumnName>();
			result.AddItem(row.head.name);
			DancingLinks.Node<ColumnName> node = row.right;
			while (node != row)
			{
				result.AddItem(node.head.name);
				node = node.right;
			}
			return result;
		}

		/// <summary>Find a solution to the problem.</summary>
		/// <param name="partial">
		/// a temporary datastructure to keep the current partial
		/// answer in
		/// </param>
		/// <param name="output">the acceptor for the results that are found</param>
		/// <returns>the number of solutions found</returns>
		private int Search(IList<DancingLinks.Node<ColumnName>> partial, DancingLinks.SolutionAcceptor
			<ColumnName> output)
		{
			int results = 0;
			if (head.right == head)
			{
				IList<IList<ColumnName>> result = new AList<IList<ColumnName>>(partial.Count);
				foreach (DancingLinks.Node<ColumnName> row in partial)
				{
					result.AddItem(GetRowName(row));
				}
				output.Solution(result);
				results += 1;
			}
			else
			{
				DancingLinks.ColumnHeader<ColumnName> col = FindBestColumn();
				if (col.size > 0)
				{
					CoverColumn(col);
					DancingLinks.Node<ColumnName> row = col.down;
					while (row != col)
					{
						partial.AddItem(row);
						DancingLinks.Node<ColumnName> node = row.right;
						while (node != row)
						{
							CoverColumn(node.head);
							node = node.right;
						}
						results += Search(partial, output);
						partial.Remove(partial.Count - 1);
						node = row.left;
						while (node != row)
						{
							UncoverColumn(node.head);
							node = node.left;
						}
						row = row.down;
					}
					UncoverColumn(col);
				}
			}
			return results;
		}

		/// <summary>Generate a list of prefixes down to a given depth.</summary>
		/// <remarks>
		/// Generate a list of prefixes down to a given depth. Assumes that the
		/// problem is always deeper than depth.
		/// </remarks>
		/// <param name="depth">the depth to explore down</param>
		/// <param name="choices">an array of length depth to describe a prefix</param>
		/// <param name="prefixes">a working datastructure</param>
		private void SearchPrefixes(int depth, int[] choices, IList<int[]> prefixes)
		{
			if (depth == 0)
			{
				prefixes.AddItem(choices.MemberwiseClone());
			}
			else
			{
				DancingLinks.ColumnHeader<ColumnName> col = FindBestColumn();
				if (col.size > 0)
				{
					CoverColumn(col);
					DancingLinks.Node<ColumnName> row = col.down;
					int rowId = 0;
					while (row != col)
					{
						DancingLinks.Node<ColumnName> node = row.right;
						while (node != row)
						{
							CoverColumn(node.head);
							node = node.right;
						}
						choices[choices.Length - depth] = rowId;
						SearchPrefixes(depth - 1, choices, prefixes);
						node = row.left;
						while (node != row)
						{
							UncoverColumn(node.head);
							node = node.left;
						}
						row = row.down;
						rowId += 1;
					}
					UncoverColumn(col);
				}
			}
		}

		/// <summary>Generate a list of row choices to cover the first moves.</summary>
		/// <param name="depth">the length of the prefixes to generate</param>
		/// <returns>a list of integer arrays that list the rows to pick in order</returns>
		public virtual IList<int[]> Split(int depth)
		{
			int[] choices = new int[depth];
			IList<int[]> result = new AList<int[]>(100000);
			SearchPrefixes(depth, choices, result);
			return result;
		}

		/// <summary>Make one move from a prefix</summary>
		/// <param name="goalRow">the row that should be choosen</param>
		/// <returns>the row that was found</returns>
		private DancingLinks.Node<ColumnName> Advance(int goalRow)
		{
			DancingLinks.ColumnHeader<ColumnName> col = FindBestColumn();
			if (col.size > 0)
			{
				CoverColumn(col);
				DancingLinks.Node<ColumnName> row = col.down;
				int id = 0;
				while (row != col)
				{
					if (id == goalRow)
					{
						DancingLinks.Node<ColumnName> node = row.right;
						while (node != row)
						{
							CoverColumn(node.head);
							node = node.right;
						}
						return row;
					}
					id += 1;
					row = row.down;
				}
			}
			return null;
		}

		/// <summary>Undo a prefix exploration</summary>
		/// <param name="row"/>
		private void Rollback(DancingLinks.Node<ColumnName> row)
		{
			DancingLinks.Node<ColumnName> node = row.left;
			while (node != row)
			{
				UncoverColumn(node.head);
				node = node.left;
			}
			UncoverColumn(row.head);
		}

		/// <summary>Given a prefix, find solutions under it.</summary>
		/// <param name="prefix">
		/// a list of row choices that control which part of the search
		/// tree to explore
		/// </param>
		/// <param name="output">the output for each solution</param>
		/// <returns>the number of solutions</returns>
		public virtual int Solve(int[] prefix, DancingLinks.SolutionAcceptor<ColumnName> 
			output)
		{
			IList<DancingLinks.Node<ColumnName>> choices = new AList<DancingLinks.Node<ColumnName
				>>();
			for (int i = 0; i < prefix.Length; ++i)
			{
				choices.AddItem(Advance(prefix[i]));
			}
			int result = Search(choices, output);
			for (int i_1 = prefix.Length - 1; i_1 >= 0; --i_1)
			{
				Rollback(choices[i_1]);
			}
			return result;
		}

		/// <summary>Solve a complete problem</summary>
		/// <param name="output">the acceptor to receive answers</param>
		/// <returns>the number of solutions</returns>
		public virtual int Solve(DancingLinks.SolutionAcceptor<ColumnName> output)
		{
			return Search(new AList<DancingLinks.Node<ColumnName>>(), output);
		}
	}
}
