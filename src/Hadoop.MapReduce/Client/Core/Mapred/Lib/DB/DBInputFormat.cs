using System;
using System.Collections.Generic;
using Java.Sql;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.DB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.DB
{
	public class DBInputFormat<T> : DBInputFormat<T>, InputFormat<LongWritable, T>, JobConfigurable
		where T : DBWritable
	{
		/// <summary>A RecordReader that reads records from a SQL table.</summary>
		/// <remarks>
		/// A RecordReader that reads records from a SQL table.
		/// Emits LongWritables containing the record number as
		/// key and DBWritables as value.
		/// </remarks>
		protected internal class DBRecordReader : Org.Apache.Hadoop.Mapreduce.Lib.DB.DBRecordReader
			<T>, RecordReader<LongWritable, T>
		{
			/// <summary>The constructor is kept to be compatible with M/R 1.x</summary>
			/// <param name="split">The InputSplit to read data for</param>
			/// <exception cref="Java.Sql.SQLException"/>
			protected internal DBRecordReader(DBInputFormat<T> _enclosing, DBInputFormat.DBInputSplit
				 split, Type inputClass, JobConf job)
				: base(split, inputClass, job, this._enclosing.connection, this._enclosing.dbConf
					, this._enclosing.conditions, this._enclosing.fieldNames, this._enclosing.tableName
					)
			{
				this._enclosing = _enclosing;
			}

			/// <param name="split">The InputSplit to read data for</param>
			/// <exception cref="Java.Sql.SQLException"></exception>
			protected internal DBRecordReader(DBInputFormat<T> _enclosing, DBInputFormat.DBInputSplit
				 split, Type inputClass, JobConf job, Connection conn, DBConfiguration dbConfig, 
				string cond, string[] fields, string table)
				: base(split, inputClass, job, conn, dbConfig, cond, fields, table)
			{
				this._enclosing = _enclosing;
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public virtual LongWritable CreateKey()
			{
				return new LongWritable();
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public override T CreateValue()
			{
				return base.CreateValue();
			}

			/// <exception cref="System.IO.IOException"/>
			public override long GetPos()
			{
				return base.GetPos();
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public override bool Next(LongWritable key, T value)
			{
				return base.Next(key, value);
			}

			private readonly DBInputFormat<T> _enclosing;
		}

		/// <summary>
		/// A RecordReader implementation that just passes through to a wrapped
		/// RecordReader built with the new API.
		/// </summary>
		private class DBRecordReaderWrapper<T> : RecordReader<LongWritable, T>
			where T : DBWritable
		{
			private DBRecordReader<T> rr;

			public DBRecordReaderWrapper(DBRecordReader<T> inner)
			{
				this.rr = inner;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				rr.Close();
			}

			public virtual LongWritable CreateKey()
			{
				return new LongWritable();
			}

			public virtual T CreateValue()
			{
				return rr.CreateValue();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual float GetProgress()
			{
				return rr.GetProgress();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetPos()
			{
				return rr.GetPos();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool Next(LongWritable key, T value)
			{
				return rr.Next(key, value);
			}
		}

		/// <summary>A Class that does nothing, implementing DBWritable</summary>
		public class NullDBWritable : DBInputFormat.NullDBWritable, DBWritable, Writable
		{
		}

		/// <summary>A InputSplit that spans a set of rows</summary>
		protected internal class DBInputSplit : DBInputFormat.DBInputSplit, InputSplit
		{
			/// <summary>Default Constructor</summary>
			public DBInputSplit()
			{
			}

			/// <summary>Convenience Constructor</summary>
			/// <param name="start">the index of the first row to select</param>
			/// <param name="end">the index of the last row to select</param>
			public DBInputSplit(long start, long end)
				: base(start, end)
			{
			}
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public virtual void Configure(JobConf job)
		{
			base.SetConf(job);
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual RecordReader<LongWritable, T> GetRecordReader(InputSplit split, JobConf
			 job, Reporter reporter)
		{
			// wrap the DBRR in a shim class to deal with API differences.
			return new DBInputFormat.DBRecordReaderWrapper<T>((DBRecordReader<T>)CreateDBRecordReader
				((DBInputFormat.DBInputSplit)split, job));
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual InputSplit[] GetSplits(JobConf job, int chunks)
		{
			IList<InputSplit> newSplits = base.GetSplits(Job.GetInstance(job));
			InputSplit[] ret = new InputSplit[newSplits.Count];
			int i = 0;
			foreach (InputSplit s in newSplits)
			{
				DBInputFormat.DBInputSplit split = (DBInputFormat.DBInputSplit)s;
				ret[i++] = new DBInputFormat.DBInputSplit(split.GetStart(), split.GetEnd());
			}
			return ret;
		}

		/// <summary>Initializes the map-part of the job with the appropriate input settings.
		/// 	</summary>
		/// <param name="job">The job</param>
		/// <param name="inputClass">
		/// the class object implementing DBWritable, which is the
		/// Java object holding tuple fields.
		/// </param>
		/// <param name="tableName">The table to read data from</param>
		/// <param name="conditions">
		/// The condition which to select data with, eg. '(updated &gt;
		/// 20070101 AND length &gt; 0)'
		/// </param>
		/// <param name="orderBy">the fieldNames in the orderBy clause.</param>
		/// <param name="fieldNames">The field names in the table</param>
		/// <seealso cref="DBInputFormat{T}.SetInput(Org.Apache.Hadoop.Mapred.JobConf, System.Type{T}, string, string)
		/// 	"/>
		public static void SetInput(JobConf job, Type inputClass, string tableName, string
			 conditions, string orderBy, params string[] fieldNames)
		{
			job.SetInputFormat(typeof(DBInputFormat));
			DBConfiguration dbConf = new DBConfiguration(job);
			dbConf.SetInputClass(inputClass);
			dbConf.SetInputTableName(tableName);
			dbConf.SetInputFieldNames(fieldNames);
			dbConf.SetInputConditions(conditions);
			dbConf.SetInputOrderBy(orderBy);
		}

		/// <summary>Initializes the map-part of the job with the appropriate input settings.
		/// 	</summary>
		/// <param name="job">The job</param>
		/// <param name="inputClass">
		/// the class object implementing DBWritable, which is the
		/// Java object holding tuple fields.
		/// </param>
		/// <param name="inputQuery">
		/// the input query to select fields. Example :
		/// "SELECT f1, f2, f3 FROM Mytable ORDER BY f1"
		/// </param>
		/// <param name="inputCountQuery">
		/// the input query that returns the number of records in
		/// the table.
		/// Example : "SELECT COUNT(f1) FROM Mytable"
		/// </param>
		/// <seealso cref="DBInputFormat{T}.SetInput(Org.Apache.Hadoop.Mapred.JobConf, System.Type{T}, string, string, string, string[])
		/// 	"/>
		public static void SetInput(JobConf job, Type inputClass, string inputQuery, string
			 inputCountQuery)
		{
			job.SetInputFormat(typeof(DBInputFormat));
			DBConfiguration dbConf = new DBConfiguration(job);
			dbConf.SetInputClass(inputClass);
			dbConf.SetInputQuery(inputQuery);
			dbConf.SetInputCountQuery(inputCountQuery);
		}
	}
}
