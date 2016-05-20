using Sharpen;

namespace org.apache.hadoop.tools
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
			LEFT,
			RIGHT
		}

		private class Column
		{
			private readonly System.Collections.Generic.List<string> rows;

			private readonly org.apache.hadoop.tools.TableListing.Justification justification;

			private readonly bool wrap;

			private int wrapWidth = int.MaxValue;

			private int maxWidth;

			internal Column(string title, org.apache.hadoop.tools.TableListing.Justification 
				justification, bool wrap)
			{
				this.rows = new System.Collections.Generic.List<string>();
				this.justification = justification;
				this.wrap = wrap;
				this.maxWidth = 0;
				addRow(title);
			}

			private void addRow(string val)
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
				rows.add(val);
			}

			private int getMaxWidth()
			{
				return maxWidth;
			}

			private void setWrapWidth(int width)
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
			internal virtual string[] getRow(int idx)
			{
				string raw = rows[idx];
				// Line-wrap if it's too long
				string[] lines = new string[] { raw };
				if (wrap)
				{
					lines = org.apache.commons.lang.WordUtils.wrap(lines[0], wrapWidth, "\n", true).split
						("\n");
				}
				for (int i = 0; i < lines.Length; i++)
				{
					if (justification == org.apache.hadoop.tools.TableListing.Justification.LEFT)
					{
						lines[i] = org.apache.commons.lang.StringUtils.rightPad(lines[i], maxWidth);
					}
					else
					{
						if (justification == org.apache.hadoop.tools.TableListing.Justification.RIGHT)
						{
							lines[i] = org.apache.commons.lang.StringUtils.leftPad(lines[i], maxWidth);
						}
					}
				}
				return lines;
			}
		}

		public class Builder
		{
			private readonly System.Collections.Generic.LinkedList<org.apache.hadoop.tools.TableListing.Column
				> columns = new System.Collections.Generic.LinkedList<org.apache.hadoop.tools.TableListing.Column
				>();

			private bool showHeader = true;

			private int wrapWidth = int.MaxValue;

			/// <summary>Create a new Builder.</summary>
			public Builder()
			{
			}

			public virtual org.apache.hadoop.tools.TableListing.Builder addField(string title
				)
			{
				return addField(title, org.apache.hadoop.tools.TableListing.Justification.LEFT, false
					);
			}

			public virtual org.apache.hadoop.tools.TableListing.Builder addField(string title
				, org.apache.hadoop.tools.TableListing.Justification justification)
			{
				return addField(title, justification, false);
			}

			public virtual org.apache.hadoop.tools.TableListing.Builder addField(string title
				, bool wrap)
			{
				return addField(title, org.apache.hadoop.tools.TableListing.Justification.LEFT, wrap
					);
			}

			/// <summary>Add a new field to the Table under construction.</summary>
			/// <param name="title">Field title.</param>
			/// <param name="justification">Right or left justification. Defaults to left.</param>
			/// <param name="wrap">
			/// Width at which to auto-wrap the content of the cell.
			/// Defaults to Integer.MAX_VALUE.
			/// </param>
			/// <returns>This Builder object</returns>
			public virtual org.apache.hadoop.tools.TableListing.Builder addField(string title
				, org.apache.hadoop.tools.TableListing.Justification justification, bool wrap)
			{
				columns.add(new org.apache.hadoop.tools.TableListing.Column(title, justification, 
					wrap));
				return this;
			}

			/// <summary>Whether to hide column headers in table output</summary>
			public virtual org.apache.hadoop.tools.TableListing.Builder hideHeaders()
			{
				this.showHeader = false;
				return this;
			}

			/// <summary>Whether to show column headers in table output.</summary>
			/// <remarks>Whether to show column headers in table output. This is the default.</remarks>
			public virtual org.apache.hadoop.tools.TableListing.Builder showHeaders()
			{
				this.showHeader = true;
				return this;
			}

			/// <summary>Set the maximum width of a row in the TableListing.</summary>
			/// <remarks>
			/// Set the maximum width of a row in the TableListing. Must have one or
			/// more wrappable fields for this to take effect.
			/// </remarks>
			public virtual org.apache.hadoop.tools.TableListing.Builder wrapWidth(int width)
			{
				this.wrapWidth = width;
				return this;
			}

			/// <summary>Create a new TableListing.</summary>
			public virtual org.apache.hadoop.tools.TableListing build()
			{
				return new org.apache.hadoop.tools.TableListing(Sharpen.Collections.ToArray(columns
					, new org.apache.hadoop.tools.TableListing.Column[0]), showHeader, wrapWidth);
			}
		}

		private readonly org.apache.hadoop.tools.TableListing.Column[] columns;

		private int numRows;

		private readonly bool showHeader;

		private readonly int wrapWidth;

		internal TableListing(org.apache.hadoop.tools.TableListing.Column[] columns, bool
			 showHeader, int wrapWidth)
		{
			this.columns = columns;
			this.numRows = 0;
			this.showHeader = showHeader;
			this.wrapWidth = wrapWidth;
		}

		/// <summary>Add a new row.</summary>
		/// <param name="row">The row of objects to add-- one per column.</param>
		public virtual void addRow(params string[] row)
		{
			if (row.Length != columns.Length)
			{
				throw new System.Exception("trying to add a row with " + row.Length + " columns, but we have "
					 + columns.Length + " columns.");
			}
			for (int i = 0; i < columns.Length; i++)
			{
				columns[i].addRow(row[i]);
			}
			numRows++;
		}

		public override string ToString()
		{
			java.lang.StringBuilder builder = new java.lang.StringBuilder();
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
					org.apache.hadoop.tools.TableListing.Column column = columns[i_1];
					if (column.wrap)
					{
						int maxWidth = column.getMaxWidth();
						if (maxWidth > 4)
						{
							column.setWrapWidth(maxWidth - 1);
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
					columnLines[j] = columns[j].getRow(i_2);
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
							builder.Append(org.apache.commons.lang.StringUtils.repeat(" ", columns[j_1].maxWidth
								));
						}
					}
					builder.Append("\n");
				}
			}
			return builder.ToString();
		}
	}
}
