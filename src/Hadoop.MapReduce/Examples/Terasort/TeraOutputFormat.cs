using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.Terasort
{
	/// <summary>An output format that writes the key and value appended together.</summary>
	public class TeraOutputFormat : FileOutputFormat<Text, Text>
	{
		internal const string FinalSyncAttribute = "mapreduce.terasort.final.sync";

		private OutputCommitter committer = null;

		/// <summary>Set the requirement for a final sync before the stream is closed.</summary>
		internal static void SetFinalSync(JobContext job, bool newValue)
		{
			job.GetConfiguration().SetBoolean(FinalSyncAttribute, newValue);
		}

		/// <summary>Does the user want a final sync at close?</summary>
		public static bool GetFinalSync(JobContext job)
		{
			return job.GetConfiguration().GetBoolean(FinalSyncAttribute, false);
		}

		internal class TeraRecordWriter : RecordWriter<Text, Text>
		{
			private bool finalSync = false;

			private FSDataOutputStream @out;

			public TeraRecordWriter(FSDataOutputStream @out, JobContext job)
			{
				finalSync = GetFinalSync(job);
				this.@out = @out;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(Text key, Text value)
			{
				lock (this)
				{
					@out.Write(key.GetBytes(), 0, key.GetLength());
					@out.Write(value.GetBytes(), 0, value.GetLength());
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close(TaskAttemptContext context)
			{
				if (finalSync)
				{
					@out.Sync();
				}
				@out.Close();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Mapred.InvalidJobConfException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void CheckOutputSpecs(JobContext job)
		{
			// Ensure that the output directory is set
			Path outDir = GetOutputPath(job);
			if (outDir == null)
			{
				throw new InvalidJobConfException("Output directory not set in JobConf.");
			}
			Configuration jobConf = job.GetConfiguration();
			// get delegation token for outDir's file system
			TokenCache.ObtainTokensForNamenodes(job.GetCredentials(), new Path[] { outDir }, 
				jobConf);
			FileSystem fs = outDir.GetFileSystem(jobConf);
			if (fs.Exists(outDir))
			{
				// existing output dir is considered empty iff its only content is the
				// partition file.
				//
				FileStatus[] outDirKids = fs.ListStatus(outDir);
				bool empty = false;
				if (outDirKids != null && outDirKids.Length == 1)
				{
					FileStatus st = outDirKids[0];
					string fname = st.GetPath().GetName();
					empty = !st.IsDirectory() && TeraInputFormat.PartitionFilename.Equals(fname);
				}
				if (TeraSort.GetUseSimplePartitioner(job) || !empty)
				{
					throw new FileAlreadyExistsException("Output directory " + outDir + " already exists"
						);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override RecordWriter<Text, Text> GetRecordWriter(TaskAttemptContext job)
		{
			Path file = GetDefaultWorkFile(job, string.Empty);
			FileSystem fs = file.GetFileSystem(job.GetConfiguration());
			FSDataOutputStream fileOut = fs.Create(file);
			return new TeraOutputFormat.TeraRecordWriter(fileOut, job);
		}

		/// <exception cref="System.IO.IOException"/>
		public override OutputCommitter GetOutputCommitter(TaskAttemptContext context)
		{
			if (committer == null)
			{
				Path output = GetOutputPath(context);
				committer = new FileOutputCommitter(output, context);
			}
			return committer;
		}
	}
}
