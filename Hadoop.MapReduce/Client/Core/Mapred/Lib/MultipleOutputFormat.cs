using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// This abstract class extends the FileOutputFormat, allowing to write the
	/// output data to different output files.
	/// </summary>
	/// <remarks>
	/// This abstract class extends the FileOutputFormat, allowing to write the
	/// output data to different output files. There are three basic use cases for
	/// this class.
	/// Case one: This class is used for a map reduce job with at least one reducer.
	/// The reducer wants to write data to different files depending on the actual
	/// keys. It is assumed that a key (or value) encodes the actual key (value)
	/// and the desired location for the actual key (value).
	/// Case two: This class is used for a map only job. The job wants to use an
	/// output file name that is either a part of the input file name of the input
	/// data, or some derivation of it.
	/// Case three: This class is used for a map only job. The job wants to use an
	/// output file name that depends on both the keys and the input file name,
	/// </remarks>
	public abstract class MultipleOutputFormat<K, V> : FileOutputFormat<K, V>
	{
		/// <summary>
		/// Create a composite record writer that can write key/value data to different
		/// output files
		/// </summary>
		/// <param name="fs">the file system to use</param>
		/// <param name="job">the job conf for the job</param>
		/// <param name="name">the leaf file name for the output file (such as part-00000")</param>
		/// <param name="arg3">a progressable for reporting progress.</param>
		/// <returns>a composite record writer</returns>
		/// <exception cref="System.IO.IOException"/>
		public override RecordWriter<K, V> GetRecordWriter(FileSystem fs, JobConf job, string
			 name, Progressable arg3)
		{
			FileSystem myFS = fs;
			string myName = GenerateLeafFileName(name);
			JobConf myJob = job;
			Progressable myProgressable = arg3;
			return new _RecordWriter_82(this, myName, myJob, myFS, myProgressable);
		}

		private sealed class _RecordWriter_82 : RecordWriter<K, V>
		{
			public _RecordWriter_82(MultipleOutputFormat<K, V> _enclosing, string myName, JobConf
				 myJob, FileSystem myFS, Progressable myProgressable)
			{
				this._enclosing = _enclosing;
				this.myName = myName;
				this.myJob = myJob;
				this.myFS = myFS;
				this.myProgressable = myProgressable;
				this.recordWriters = new SortedDictionary<string, RecordWriter<K, V>>();
			}

			internal SortedDictionary<string, RecordWriter<K, V>> recordWriters;

			// a cache storing the record writers for different output files.
			/// <exception cref="System.IO.IOException"/>
			public void Write(K key, V value)
			{
				// get the file name based on the key
				string keyBasedPath = this._enclosing.GenerateFileNameForKeyValue(key, value, myName
					);
				// get the file name based on the input file name
				string finalPath = this._enclosing.GetInputFileBasedOutputFileName(myJob, keyBasedPath
					);
				// get the actual key
				K actualKey = this._enclosing.GenerateActualKey(key, value);
				V actualValue = this._enclosing.GenerateActualValue(key, value);
				RecordWriter<K, V> rw = this.recordWriters[finalPath];
				if (rw == null)
				{
					// if we don't have the record writer yet for the final path, create
					// one
					// and add it to the cache
					rw = this._enclosing.GetBaseRecordWriter(myFS, myJob, finalPath, myProgressable);
					this.recordWriters[finalPath] = rw;
				}
				rw.Write(actualKey, actualValue);
			}

			/// <exception cref="System.IO.IOException"/>
			public void Close(Reporter reporter)
			{
				IEnumerator<string> keys = this.recordWriters.Keys.GetEnumerator();
				while (keys.HasNext())
				{
					RecordWriter<K, V> rw = this.recordWriters[keys.Next()];
					rw.Close(reporter);
				}
				this.recordWriters.Clear();
			}

			private readonly MultipleOutputFormat<K, V> _enclosing;

			private readonly string myName;

			private readonly JobConf myJob;

			private readonly FileSystem myFS;

			private readonly Progressable myProgressable;
		}

		/// <summary>Generate the leaf name for the output file name.</summary>
		/// <remarks>
		/// Generate the leaf name for the output file name. The default behavior does
		/// not change the leaf file name (such as part-00000)
		/// </remarks>
		/// <param name="name">the leaf file name for the output file</param>
		/// <returns>the given leaf file name</returns>
		protected internal virtual string GenerateLeafFileName(string name)
		{
			return name;
		}

		/// <summary>
		/// Generate the file output file name based on the given key and the leaf file
		/// name.
		/// </summary>
		/// <remarks>
		/// Generate the file output file name based on the given key and the leaf file
		/// name. The default behavior is that the file name does not depend on the
		/// key.
		/// </remarks>
		/// <param name="key">the key of the output data</param>
		/// <param name="name">the leaf file name</param>
		/// <returns>generated file name</returns>
		protected internal virtual string GenerateFileNameForKeyValue(K key, V value, string
			 name)
		{
			return name;
		}

		/// <summary>Generate the actual key from the given key/value.</summary>
		/// <remarks>
		/// Generate the actual key from the given key/value. The default behavior is that
		/// the actual key is equal to the given key
		/// </remarks>
		/// <param name="key">the key of the output data</param>
		/// <param name="value">the value of the output data</param>
		/// <returns>the actual key derived from the given key/value</returns>
		protected internal virtual K GenerateActualKey(K key, V value)
		{
			return key;
		}

		/// <summary>Generate the actual value from the given key and value.</summary>
		/// <remarks>
		/// Generate the actual value from the given key and value. The default behavior is that
		/// the actual value is equal to the given value
		/// </remarks>
		/// <param name="key">the key of the output data</param>
		/// <param name="value">the value of the output data</param>
		/// <returns>the actual value derived from the given key/value</returns>
		protected internal virtual V GenerateActualValue(K key, V value)
		{
			return value;
		}

		/// <summary>Generate the outfile name based on a given anme and the input file name.
		/// 	</summary>
		/// <remarks>
		/// Generate the outfile name based on a given anme and the input file name. If
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.MapInputFile"/>
		/// does not exists (i.e. this is not for a map only job),
		/// the given name is returned unchanged. If the config value for
		/// "num.of.trailing.legs.to.use" is not set, or set 0 or negative, the given
		/// name is returned unchanged. Otherwise, return a file name consisting of the
		/// N trailing legs of the input file name where N is the config value for
		/// "num.of.trailing.legs.to.use".
		/// </remarks>
		/// <param name="job">the job config</param>
		/// <param name="name">the output file name</param>
		/// <returns>the outfile name based on a given anme and the input file name.</returns>
		protected internal virtual string GetInputFileBasedOutputFileName(JobConf job, string
			 name)
		{
			string infilepath = job.Get(MRJobConfig.MapInputFile);
			if (infilepath == null)
			{
				// if the {@link JobContext#MAP_INPUT_FILE} does not exists,
				// then return the given name
				return name;
			}
			int numOfTrailingLegsToUse = job.GetInt("mapred.outputformat.numOfTrailingLegs", 
				0);
			if (numOfTrailingLegsToUse <= 0)
			{
				return name;
			}
			Path infile = new Path(infilepath);
			Path parent = infile.GetParent();
			string midName = infile.GetName();
			Path outPath = new Path(midName);
			for (int i = 1; i < numOfTrailingLegsToUse; i++)
			{
				if (parent == null)
				{
					break;
				}
				midName = parent.GetName();
				if (midName.Length == 0)
				{
					break;
				}
				parent = parent.GetParent();
				outPath = new Path(midName, outPath);
			}
			return outPath.ToString();
		}

		/// <param name="fs">the file system to use</param>
		/// <param name="job">a job conf object</param>
		/// <param name="name">
		/// the name of the file over which a record writer object will be
		/// constructed
		/// </param>
		/// <param name="arg3">a progressable object</param>
		/// <returns>A RecordWriter object over the given file</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract RecordWriter<K, V> GetBaseRecordWriter(FileSystem fs, 
			JobConf job, string name, Progressable arg3);
	}
}
