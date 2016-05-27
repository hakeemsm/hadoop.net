using System.Collections.Generic;
using System.Text;
using Org.Apache.Commons.Lang;
using Sharpen;

namespace Org.Apache.Hadoop.Tools
{
	/// <summary>This class implements a "table listing" with column headers.</summary>
	/// <remarks>
	/// This class implements a "table listing" with column headers.
	/// Example:
	/// NAME   OWNER   GROUP   MODE       WEIGHT
	/// pool1  andrew  andrew  rwxr-xr-x     100
	/// pool2  andrew  andrew  rwxr-xr-x     100
	/// pool3  andrew  andrew  rwxr-xr-x     100
	/// </remarks>
	public class TableListing
	{
		public enum Justification
		{
			Left,
			Right
		}

		private class Column
		{
			private readonly AList<string> rows;

			private readonly TableListing.Justification justification;

			private readonly bool wrap;

			private int wrapWidth = int.MaxValue;

			private int maxWidth;

			internal Column(string title, TableListing.Justification justification, bool wrap
				)
			{
				this.rows = new AList<string>();
				this.justification = justification;
				this.wrap = wrap;
				this.maxWidth = 0;
				AddRow(title);
			}

			private void AddRow(string val)
			{
				if (val == null)
				{
					val = string.Empty;
				}
				if ((val.Length + 1) > maxWidth)
				{
					maxWidth = val.Length + 1;
				}
				// Ceiling at wrapWidth, because it'll get wrapped
				if (maxWidth > wrapWidth)
				{
					maxWidth = wrapWidth;
				}
				rows.AddItem(val);
			}

			private int GetMaxWidth()
			{
				return maxWidth;
			}

			private void SetWrapWidth(int width)
			{
				wrapWidth = width;
				// Ceiling the maxLength at wrapWidth
				if (maxWidth > wrapWidth)
				{
					maxWidth = wrapWidth;
				}
				else
				{
					// Else we need to traverse through and find the real maxWidth
					maxWidth = 0;
					for (int i = 0; i < rows.Count; i++)
					{
						int length = rows[i].Length;
						if (length > maxWidth)
						{
							maxWidth = length;
						}
					}
				}
			}

			/// <summary>
			/// Return the ith row of the column as a set of wrapped strings, each at
			/// most wrapWidth in length.
			/// </summary>
			internal virtual string[] GetRow(int idx)
			{
				string raw = rows[idx];
				// Line-wrap if it's too long
				string[] lines = new string[] { raw };
				if (wrap)
				{
					lines = WordUtils.Wrap(lines[0], wrapWidth, "\n", true).Split("\n");
				}
				for (int i = 0; i < lines.Length; i++)
				{
					if (justification == TableListing.Justification.Left)
					{
						lines[i] = StringUtils.RightPad(lines[i], maxWidth);
					}
					else
					{
						if (justification == TableListing.Justification.Right)
						{
							lines[i] = StringUtils.LeftPad(lines[i], maxWidth);
						}
					}
				}
				return lines;
			}
		}

		public class Builder
		{
			private readonly List<TableListing.Column> columns = new List<TableListing.Column
				>();

			private bool showHeader = true;

			private int wrapWidth = int.MaxValue;

			/// <summary>Create a new Builder.</summary>
			public Builder()
			{
			}

			public virtual TableListing.Builder AddField(string title)
			{
				return AddField(title, TableListing.Justification.Left, false);
			}

			public virtual TableListing.Builder AddField(string title, TableListing.Justification
				 justification)
			{
				return AddField(title, justification, false);
			}

			public virtual TableListing.Builder AddField(string title, bool wrap)
			{
				return AddField(title, TableListing.Justification.Left, wrap);
			}

			/// <summary>Add a new field to the Table under construction.</summary>
			/// <param name="title">Field title.</param>
			/// <param name="justification">Right or left justification. Defaults to left.</param>
			/// <param name="wrap">
			/// Width at which to auto-wrap the content of the cell.
			/// Defaults to Integer.MAX_VALUE.
			/// </param>
			/// <returns>This Builder object</returns>
			public virtual TableListing.Builder AddField(string title, TableListing.Justification
				 justification, bool wrap)
			{
				columns.AddItem(new TableListing.Column(title, justification, wrap));
				return this;
			}

			/// <summary>Whether to hide column headers in table output</summary>
			public virtual TableListing.Builder HideHeaders()
			{
				this.showHeader = false;
				return this;
			}

			/// <summary>Whether to show column headers in table output.</summary>
			/// <remarks>Whether to show column headers in table output. This is the default.</remarks>
			public virtual TableListing.Builder ShowHeaders()
			{
				this.showHeader = true;
				return this;
			}

			/// <summary>Set the maximum width of a row in the TableListing.</summary>
			/// <remarks>
			/// Set the maximum width of a row in the TableListing. Must have one or
			/// more wrappable fields for this to take effect.
			/// </remarks>
			public virtual TableListing.Builder WrapWidth(int width)
			{
				this.wrapWidth = width;
				return this;
			}

			/// <summary>Create a new TableListing.</summary>
			public virtual TableListing Build()
			{
				return new TableListing(Sharpen.Collections.ToArray(columns, new TableListing.Column
					[0]), showHeader, wrapWidth);
			}
		}

		private readonly TableListing.Column[] columns;

		private int numRows;

		private readonly bool showHeader;

		private readonly int wrapWidth;

		internal TableListing(TableListing.Column[] columns, bool showHeader, int wrapWidth
			)
		{
			this.columns = columns;
			this.numRows = 0;
			this.showHeader = showHeader;
			this.wrapWidth = wrapWidth;
		}

		/// <summary>Add a new row.</summary>
		/// <param name="row">The row of objects to add-- one per column.</param>
		public virtual void AddRow(params string[] row)
		{
			if (row.Length != columns.Length)
			{
				throw new RuntimeException("trying to add a row with " + row.Length + " columns, but we have "
					 + columns.Length + " columns.");
			}
			for (int i = 0; i < columns.Length; i++)
			{
				columns[i].AddRow(row[i]);
			}
			numRows++;
		}

		public override string ToString()
		{
			StringBuilder builder = new StringBuilder();
			// Calculate the widths of each column based on their maxWidths and
			// the wrapWidth for the entire table
			int width = (columns.Length - 1) * 2;
			// inter-column padding
			for (int i = 0; i < columns.Length; i++)
			{
				width += columns[i].maxWidth;
			}
			// Decrease the column size of wrappable columns until the goal width
			// is reached, or we can't decrease anymore
			while (width > wrapWidth)
			{
				bool modified = false;
				for (int i_1 = 0; i_1 < columns.Length; i_1++)
				{
					TableListing.Column column = columns[i_1];
					if (column.wrap)
					{
						int maxWidth = column.GetMaxWidth();
						if (maxWidth > 4)
						{
							column.SetWrapWidth(maxWidth - 1);
							modified = true;
							width -= 1;
							if (width <= wrapWidth)
							{
								break;
							}
						}
					}
				}
				if (!modified)
				{
					break;
				}
			}
			int startrow = 0;
			if (!showHeader)
			{
				startrow = 1;
			}
			string[][] columnLines = new string[columns.Length][];
			for (int i_2 = startrow; i_2 < numRows + 1; i_2++)
			{
				int maxColumnLines = 0;
				for (int j = 0; j < columns.Length; j++)
				{
					columnLines[j] = columns[j].GetRow(i_2);
					if (columnLines[j].Length > maxColumnLines)
					{
						maxColumnLines = columnLines[j].Length;
					}
				}
				for (int c = 0; c < maxColumnLines; c++)
				{
					// First column gets no left-padding
					string prefix = string.Empty;
					for (int j_1 = 0; j_1 < columns.Length; j_1++)
					{
						// Prepend padding
						builder.Append(prefix);
						prefix = " ";
						if (columnLines[j_1].Length > c)
						{
							builder.Append(columnLines[j_1][c]);
						}
						else
						{
							builder.Append(StringUtils.Repeat(" ", columns[j_1].maxWidth));
						}
					}
					builder.Append("\n");
				}
			}
			return builder.ToString();
		}
	}
}
