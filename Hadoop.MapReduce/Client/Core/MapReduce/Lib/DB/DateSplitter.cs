using System;
using System.Collections.Generic;
using Java.Sql;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>Implement DBSplitter over date/time values.</summary>
	/// <remarks>
	/// Implement DBSplitter over date/time values.
	/// Make use of logic from IntegerSplitter, since date/time are just longs
	/// in Java.
	/// </remarks>
	public class DateSplitter : IntegerSplitter
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(DateSplitter));

		/// <exception cref="Java.Sql.SQLException"/>
		public override IList<InputSplit> Split(Configuration conf, ResultSet results, string
			 colName)
		{
			long minVal;
			long maxVal;
			int sqlDataType = results.GetMetaData().GetColumnType(1);
			minVal = ResultSetColToLong(results, 1, sqlDataType);
			maxVal = ResultSetColToLong(results, 2, sqlDataType);
			string lowClausePrefix = colName + " >= ";
			string highClausePrefix = colName + " < ";
			int numSplits = conf.GetInt(MRJobConfig.NumMaps, 1);
			if (numSplits < 1)
			{
				numSplits = 1;
			}
			if (minVal == long.MinValue && maxVal == long.MinValue)
			{
				// The range of acceptable dates is NULL to NULL. Just create a single split.
				IList<InputSplit> splits = new AList<InputSplit>();
				splits.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL"
					, colName + " IS NULL"));
				return splits;
			}
			// Gather the split point integers
			IList<long> splitPoints = Split(numSplits, minVal, maxVal);
			IList<InputSplit> splits_1 = new AList<InputSplit>();
			// Turn the split points into a set of intervals.
			long start = splitPoints[0];
			DateTime startDate = LongToDate(start, sqlDataType);
			if (sqlDataType == Types.Timestamp)
			{
				// The lower bound's nanos value needs to match the actual lower-bound nanos.
				try
				{
					((Timestamp)startDate).SetNanos(results.GetTimestamp(1).GetNanos());
				}
				catch (ArgumentNullException)
				{
				}
			}
			// If the lower bound was NULL, we'll get an NPE; just ignore it and don't set nanos.
			for (int i = 1; i < splitPoints.Count; i++)
			{
				long end = splitPoints[i];
				DateTime endDate = LongToDate(end, sqlDataType);
				if (i == splitPoints.Count - 1)
				{
					if (sqlDataType == Types.Timestamp)
					{
						// The upper bound's nanos value needs to match the actual upper-bound nanos.
						try
						{
							((Timestamp)endDate).SetNanos(results.GetTimestamp(2).GetNanos());
						}
						catch (ArgumentNullException)
						{
						}
					}
					// If the upper bound was NULL, we'll get an NPE; just ignore it and don't set nanos.
					// This is the last one; use a closed interval.
					splits_1.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix
						 + DateToString(startDate), colName + " <= " + DateToString(endDate)));
				}
				else
				{
					// Normal open-interval case.
					splits_1.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix
						 + DateToString(startDate), highClausePrefix + DateToString(endDate)));
				}
				start = end;
				startDate = endDate;
			}
			if (minVal == long.MinValue || maxVal == long.MinValue)
			{
				// Add an extra split to handle the null case that we saw.
				splits_1.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL"
					, colName + " IS NULL"));
			}
			return splits_1;
		}

		/// <summary>
		/// Retrieve the value from the column in a type-appropriate manner and return
		/// its timestamp since the epoch.
		/// </summary>
		/// <remarks>
		/// Retrieve the value from the column in a type-appropriate manner and return
		/// its timestamp since the epoch. If the column is null, then return Long.MIN_VALUE.
		/// This will cause a special split to be generated for the NULL case, but may also
		/// cause poorly-balanced splits if most of the actual dates are positive time
		/// since the epoch, etc.
		/// </remarks>
		/// <exception cref="Java.Sql.SQLException"/>
		private long ResultSetColToLong(ResultSet rs, int colNum, int sqlDataType)
		{
			try
			{
				switch (sqlDataType)
				{
					case Types.Date:
					{
						return rs.GetDate(colNum).GetTime();
					}

					case Types.Time:
					{
						return rs.GetTime(colNum).GetTime();
					}

					case Types.Timestamp:
					{
						return rs.GetTimestamp(colNum).GetTime();
					}

					default:
					{
						throw new SQLException("Not a date-type field");
					}
				}
			}
			catch (ArgumentNullException)
			{
				// null column. return minimum long value.
				Log.Warn("Encountered a NULL date in the split column. Splits may be poorly balanced."
					);
				return long.MinValue;
			}
		}

		/// <summary>Parse the long-valued timestamp into the appropriate SQL date type.</summary>
		private DateTime LongToDate(long val, int sqlDataType)
		{
			switch (sqlDataType)
			{
				case Types.Date:
				{
					return Sharpen.Extensions.CreateDate(val);
				}

				case Types.Time:
				{
					return Sharpen.Extensions.CreateDate(val);
				}

				case Types.Timestamp:
				{
					return Sharpen.Extensions.CreateDate(val);
				}

				default:
				{
					// Shouldn't ever hit this case.
					return null;
				}
			}
		}

		/// <summary>
		/// Given a Date 'd', format it as a string for use in a SQL date
		/// comparison operation.
		/// </summary>
		/// <param name="d">the date to format.</param>
		/// <returns>
		/// the string representing this date in SQL with any appropriate
		/// quotation characters, etc.
		/// </returns>
		protected internal virtual string DateToString(DateTime d)
		{
			return "'" + d.ToString() + "'";
		}
	}
}
