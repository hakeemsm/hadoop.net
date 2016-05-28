using System;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	/// <summary>
	/// A base class for
	/// <see cref="Org.Apache.Hadoop.Mapreduce.OutputFormat{K, V}"/>
	/// s that read from
	/// <see cref="Org.Apache.Hadoop.FS.FileSystem"/>
	/// s.
	/// </summary>
	public abstract class FileOutputFormat<K, V> : OutputFormat<K, V>
	{
		/// <summary>
		/// Construct output file names so that, when an output directory listing is
		/// sorted lexicographically, positions correspond to output partitions.
		/// </summary>
		private static readonly NumberFormat NumberFormat = NumberFormat.GetInstance();

		protected internal const string BaseOutputName = "mapreduce.output.basename";

		protected internal const string Part = "part";

		static FileOutputFormat()
		{
			NumberFormat.SetMinimumIntegerDigits(5);
			NumberFormat.SetGroupingUsed(false);
		}

		private FileOutputCommitter committer = null;

		public const string Compress = "mapreduce.output.fileoutputformat.compress";

		public const string CompressCodec = "mapreduce.output.fileoutputformat.compress.codec";

		public const string CompressType = "mapreduce.output.fileoutputformat.compress.type";

		public const string Outdir = "mapreduce.output.fileoutputformat.outputdir";

		public enum Counter
		{
			BytesWritten
		}

		/// <summary>Set whether the output of the job is compressed.</summary>
		/// <param name="job">the job to modify</param>
		/// <param name="compress">should the output of the job be compressed?</param>
		public static void SetCompressOutput(Job job, bool compress)
		{
			job.GetConfiguration().SetBoolean(FileOutputFormat.Compress, compress);
		}

		/// <summary>Is the job output compressed?</summary>
		/// <param name="job">the Job to look in</param>
		/// <returns>
		/// <code>true</code> if the job output should be compressed,
		/// <code>false</code> otherwise
		/// </returns>
		public static bool GetCompressOutput(JobContext job)
		{
			return job.GetConfiguration().GetBoolean(FileOutputFormat.Compress, false);
		}

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.IO.Compress.CompressionCodec"/>
		/// to be used to compress job outputs.
		/// </summary>
		/// <param name="job">the job to modify</param>
		/// <param name="codecClass">
		/// the
		/// <see cref="Org.Apache.Hadoop.IO.Compress.CompressionCodec"/>
		/// to be used to
		/// compress the job outputs
		/// </param>
		public static void SetOutputCompressorClass(Job job, Type codecClass)
		{
			SetCompressOutput(job, true);
			job.GetConfiguration().SetClass(FileOutputFormat.CompressCodec, codecClass, typeof(
				CompressionCodec));
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.IO.Compress.CompressionCodec"/>
		/// for compressing the job outputs.
		/// </summary>
		/// <param name="job">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Job"/>
		/// to look in
		/// </param>
		/// <param name="defaultValue">
		/// the
		/// <see cref="Org.Apache.Hadoop.IO.Compress.CompressionCodec"/>
		/// to return if not set
		/// </param>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.IO.Compress.CompressionCodec"/>
		/// to be used to compress the
		/// job outputs
		/// </returns>
		/// <exception cref="System.ArgumentException">if the class was specified, but not found
		/// 	</exception>
		public static Type GetOutputCompressorClass(JobContext job, Type defaultValue)
		{
			Type codecClass = defaultValue;
			Configuration conf = job.GetConfiguration();
			string name = conf.Get(FileOutputFormat.CompressCodec);
			if (name != null)
			{
				try
				{
					codecClass = conf.GetClassByName(name).AsSubclass<CompressionCodec>();
				}
				catch (TypeLoadException e)
				{
					throw new ArgumentException("Compression codec " + name + " was not found.", e);
				}
			}
			return codecClass;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract override RecordWriter<K, V> GetRecordWriter(TaskAttemptContext job
			);

		/// <exception cref="Org.Apache.Hadoop.Mapred.FileAlreadyExistsException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void CheckOutputSpecs(JobContext job)
		{
			// Ensure that the output directory is set and not already there
			Path outDir = GetOutputPath(job);
			if (outDir == null)
			{
				throw new InvalidJobConfException("Output directory not set.");
			}
			// get delegation token for outDir's file system
			TokenCache.ObtainTokensForNamenodes(job.GetCredentials(), new Path[] { outDir }, 
				job.GetConfiguration());
			if (outDir.GetFileSystem(job.GetConfiguration()).Exists(outDir))
			{
				throw new FileAlreadyExistsException("Output directory " + outDir + " already exists"
					);
			}
		}

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// of the output directory for the map-reduce job.
		/// </summary>
		/// <param name="job">The job to modify</param>
		/// <param name="outputDir">
		/// the
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// of the output directory for
		/// the map-reduce job.
		/// </param>
		public static void SetOutputPath(Job job, Path outputDir)
		{
			try
			{
				outputDir = outputDir.GetFileSystem(job.GetConfiguration()).MakeQualified(outputDir
					);
			}
			catch (IOException e)
			{
				// Throw the IOException as a RuntimeException to be compatible with MR1
				throw new RuntimeException(e);
			}
			job.GetConfiguration().Set(FileOutputFormat.Outdir, outputDir.ToString());
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// to the output directory for the map-reduce job.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// to the output directory for the map-reduce job.
		/// </returns>
		/// <seealso cref="FileOutputFormat{K, V}.GetWorkOutputPath(Org.Apache.Hadoop.Mapreduce.TaskInputOutputContext{KEYIN, VALUEIN, KEYOUT, VALUEOUT})
		/// 	"/>
		public static Path GetOutputPath(JobContext job)
		{
			string name = job.GetConfiguration().Get(FileOutputFormat.Outdir);
			return name == null ? null : new Path(name);
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// to the task's temporary output directory
		/// for the map-reduce job
		/// <b id="SideEffectFiles">Tasks' Side-Effect Files</b>
		/// <p>Some applications need to create/write-to side-files, which differ from
		/// the actual job-outputs.
		/// <p>In such cases there could be issues with 2 instances of the same TIP
		/// (running simultaneously e.g. speculative tasks) trying to open/write-to the
		/// same file (path) on HDFS. Hence the application-writer will have to pick
		/// unique names per task-attempt (e.g. using the attemptid, say
		/// <tt>attempt_200709221812_0001_m_000000_0</tt>), not just per TIP.</p>
		/// <p>To get around this the Map-Reduce framework helps the application-writer
		/// out by maintaining a special
		/// <tt>${mapreduce.output.fileoutputformat.outputdir}/_temporary/_${taskid}</tt>
		/// sub-directory for each task-attempt on HDFS where the output of the
		/// task-attempt goes. On successful completion of the task-attempt the files
		/// in the <tt>${mapreduce.output.fileoutputformat.outputdir}/_temporary/_${taskid}</tt> (only)
		/// are <i>promoted</i> to <tt>${mapreduce.output.fileoutputformat.outputdir}</tt>. Of course, the
		/// framework discards the sub-directory of unsuccessful task-attempts. This
		/// is completely transparent to the application.</p>
		/// <p>The application-writer can take advantage of this by creating any
		/// side-files required in a work directory during execution
		/// of his task i.e. via
		/// <see cref="FileOutputFormat{K, V}.GetWorkOutputPath(Org.Apache.Hadoop.Mapreduce.TaskInputOutputContext{KEYIN, VALUEIN, KEYOUT, VALUEOUT})
		/// 	"/>
		/// , and
		/// the framework will move them out similarly - thus she doesn't have to pick
		/// unique paths per task-attempt.</p>
		/// <p>The entire discussion holds true for maps of jobs with
		/// reducer=NONE (i.e. 0 reduces) since output of the map, in that case,
		/// goes directly to HDFS.</p>
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// to the task's temporary output directory
		/// for the map-reduce job.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static Path GetWorkOutputPath<_T0>(TaskInputOutputContext<_T0> context)
		{
			FileOutputCommitter committer = (FileOutputCommitter)context.GetOutputCommitter();
			return committer.GetWorkPath();
		}

		/// <summary>
		/// Helper function to generate a
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// for a file that is unique for
		/// the task within the job output directory.
		/// <p>The path can be used to create custom files from within the map and
		/// reduce tasks. The path name will be unique for each task. The path parent
		/// will be the job output directory.</p>ls
		/// <p>This method uses the
		/// <see cref="FileOutputFormat{K, V}.GetUniqueFile(Org.Apache.Hadoop.Mapreduce.TaskAttemptContext, string, string)
		/// 	"/>
		/// method to make the file name
		/// unique for the task.</p>
		/// </summary>
		/// <param name="context">the context for the task.</param>
		/// <param name="name">the name for the file.</param>
		/// <param name="extension">the extension for the file</param>
		/// <returns>a unique path accross all tasks of the job.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static Path GetPathForWorkFile<_T0>(TaskInputOutputContext<_T0> context, string
			 name, string extension)
		{
			return new Path(GetWorkOutputPath(context), GetUniqueFile(context, name, extension
				));
		}

		/// <summary>Generate a unique filename, based on the task id, name, and extension</summary>
		/// <param name="context">the task that is calling this</param>
		/// <param name="name">the base filename</param>
		/// <param name="extension">the filename extension</param>
		/// <returns>a string like $name-[mrsct]-$id$extension</returns>
		public static string GetUniqueFile(TaskAttemptContext context, string name, string
			 extension)
		{
			lock (typeof(FileOutputFormat))
			{
				TaskID taskId = context.GetTaskAttemptID().GetTaskID();
				int partition = taskId.GetId();
				StringBuilder result = new StringBuilder();
				result.Append(name);
				result.Append('-');
				result.Append(TaskID.GetRepresentingCharacter(taskId.GetTaskType()));
				result.Append('-');
				result.Append(NumberFormat.Format(partition));
				result.Append(extension);
				return result.ToString();
			}
		}

		/// <summary>Get the default path and filename for the output format.</summary>
		/// <param name="context">the task context</param>
		/// <param name="extension">an extension to add to the filename</param>
		/// <returns>a full path $output/_temporary/$taskid/part-[mr]-$id</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetDefaultWorkFile(TaskAttemptContext context, string extension
			)
		{
			FileOutputCommitter committer = (FileOutputCommitter)GetOutputCommitter(context);
			return new Path(committer.GetWorkPath(), GetUniqueFile(context, GetOutputName(context
				), extension));
		}

		/// <summary>Get the base output name for the output file.</summary>
		protected internal static string GetOutputName(JobContext job)
		{
			return job.GetConfiguration().Get(BaseOutputName, Part);
		}

		/// <summary>Set the base output name for output file to be created.</summary>
		protected internal static void SetOutputName(JobContext job, string name)
		{
			job.GetConfiguration().Set(BaseOutputName, name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override OutputCommitter GetOutputCommitter(TaskAttemptContext context)
		{
			lock (this)
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
}
