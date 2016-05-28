using System;
using System.IO;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// A base class for
	/// <see cref="OutputFormat{K, V}"/>
	/// .
	/// </summary>
	public abstract class FileOutputFormat<K, V> : OutputFormat<K, V>
	{
		public enum Counter
		{
			BytesWritten
		}

		/// <summary>Set whether the output of the job is compressed.</summary>
		/// <param name="conf">
		/// the
		/// <see cref="JobConf"/>
		/// to modify
		/// </param>
		/// <param name="compress">should the output of the job be compressed?</param>
		public static void SetCompressOutput(JobConf conf, bool compress)
		{
			conf.SetBoolean(FileOutputFormat.Compress, compress);
		}

		/// <summary>Is the job output compressed?</summary>
		/// <param name="conf">
		/// the
		/// <see cref="JobConf"/>
		/// to look in
		/// </param>
		/// <returns>
		/// <code>true</code> if the job output should be compressed,
		/// <code>false</code> otherwise
		/// </returns>
		public static bool GetCompressOutput(JobConf conf)
		{
			return conf.GetBoolean(FileOutputFormat.Compress, false);
		}

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.IO.Compress.CompressionCodec"/>
		/// to be used to compress job outputs.
		/// </summary>
		/// <param name="conf">
		/// the
		/// <see cref="JobConf"/>
		/// to modify
		/// </param>
		/// <param name="codecClass">
		/// the
		/// <see cref="Org.Apache.Hadoop.IO.Compress.CompressionCodec"/>
		/// to be used to
		/// compress the job outputs
		/// </param>
		public static void SetOutputCompressorClass(JobConf conf, Type codecClass)
		{
			SetCompressOutput(conf, true);
			conf.SetClass(FileOutputFormat.CompressCodec, codecClass, typeof(CompressionCodec
				));
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.IO.Compress.CompressionCodec"/>
		/// for compressing the job outputs.
		/// </summary>
		/// <param name="conf">
		/// the
		/// <see cref="JobConf"/>
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
		public static Type GetOutputCompressorClass(JobConf conf, Type defaultValue)
		{
			Type codecClass = defaultValue;
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
		public abstract RecordWriter<K, V> GetRecordWriter(FileSystem ignored, JobConf job
			, string name, Progressable progress);

		/// <exception cref="Org.Apache.Hadoop.Mapred.FileAlreadyExistsException"/>
		/// <exception cref="Org.Apache.Hadoop.Mapred.InvalidJobConfException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CheckOutputSpecs(FileSystem ignored, JobConf job)
		{
			// Ensure that the output directory is set and not already there
			Path outDir = GetOutputPath(job);
			if (outDir == null && job.GetNumReduceTasks() != 0)
			{
				throw new InvalidJobConfException("Output directory not set in JobConf.");
			}
			if (outDir != null)
			{
				FileSystem fs = outDir.GetFileSystem(job);
				// normalize the output directory
				outDir = fs.MakeQualified(outDir);
				SetOutputPath(job, outDir);
				// get delegation token for the outDir's file system
				TokenCache.ObtainTokensForNamenodes(job.GetCredentials(), new Path[] { outDir }, 
					job);
				// check its existence
				if (fs.Exists(outDir))
				{
					throw new FileAlreadyExistsException("Output directory " + outDir + " already exists"
						);
				}
			}
		}

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// of the output directory for the map-reduce job.
		/// </summary>
		/// <param name="conf">The configuration of the job.</param>
		/// <param name="outputDir">
		/// the
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// of the output directory for
		/// the map-reduce job.
		/// </param>
		public static void SetOutputPath(JobConf conf, Path outputDir)
		{
			outputDir = new Path(conf.GetWorkingDirectory(), outputDir);
			conf.Set(FileOutputFormat.Outdir, outputDir.ToString());
		}

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// of the task's temporary output directory
		/// for the map-reduce job.
		/// <p><i>Note</i>: Task output path is set by the framework.
		/// </p>
		/// </summary>
		/// <param name="conf">The configuration of the job.</param>
		/// <param name="outputDir">
		/// the
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// of the output directory
		/// for the map-reduce job.
		/// </param>
		[InterfaceAudience.Private]
		public static void SetWorkOutputPath(JobConf conf, Path outputDir)
		{
			outputDir = new Path(conf.GetWorkingDirectory(), outputDir);
			conf.Set(JobContext.TaskOutputDir, outputDir.ToString());
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
		/// <seealso cref="FileOutputFormat{K, V}.GetWorkOutputPath(JobConf)"/>
		public static Path GetOutputPath(JobConf conf)
		{
			string name = conf.Get(FileOutputFormat.Outdir);
			return name == null ? null : new Path(name);
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// to the task's temporary output directory
		/// for the map-reduce job
		/// <b id="SideEffectFiles">Tasks' Side-Effect Files</b>
		/// <p><i>Note:</i> The following is valid only if the
		/// <see cref="OutputCommitter"/>
		/// is
		/// <see cref="FileOutputCommitter"/>
		/// . If <code>OutputCommitter</code> is not
		/// a <code>FileOutputCommitter</code>, the task's temporary output
		/// directory is same as
		/// <see cref="FileOutputFormat{K, V}.GetOutputPath(JobConf)"/>
		/// i.e.
		/// <tt>${mapreduce.output.fileoutputformat.outputdir}$</tt></p>
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
		/// side-files required in <tt>${mapreduce.task.output.dir}</tt> during execution
		/// of his reduce-task i.e. via
		/// <see cref="FileOutputFormat{K, V}.GetWorkOutputPath(JobConf)"/>
		/// , and the
		/// framework will move them out similarly - thus she doesn't have to pick
		/// unique paths per task-attempt.</p>
		/// <p><i>Note</i>: the value of <tt>${mapreduce.task.output.dir}</tt> during
		/// execution of a particular task-attempt is actually
		/// <tt>${mapreduce.output.fileoutputformat.outputdir}/_temporary/_{$taskid}</tt>, and this value is
		/// set by the map-reduce framework. So, just create any side-files in the
		/// path  returned by
		/// <see cref="FileOutputFormat{K, V}.GetWorkOutputPath(JobConf)"/>
		/// from map/reduce
		/// task to take advantage of this feature.</p>
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
		public static Path GetWorkOutputPath(JobConf conf)
		{
			string name = conf.Get(JobContext.TaskOutputDir);
			return name == null ? null : new Path(name);
		}

		/// <summary>
		/// Helper function to create the task's temporary output directory and
		/// return the path to the task's output file.
		/// </summary>
		/// <param name="conf">job-configuration</param>
		/// <param name="name">temporary task-output filename</param>
		/// <returns>path to the task's temporary output file</returns>
		/// <exception cref="System.IO.IOException"/>
		public static Path GetTaskOutputPath(JobConf conf, string name)
		{
			// ${mapred.out.dir}
			Path outputPath = GetOutputPath(conf);
			if (outputPath == null)
			{
				throw new IOException("Undefined job output-path");
			}
			OutputCommitter committer = conf.GetOutputCommitter();
			Path workPath = outputPath;
			TaskAttemptContext context = new TaskAttemptContextImpl(conf, ((TaskAttemptID)TaskAttemptID
				.ForName(conf.Get(JobContext.TaskAttemptId))));
			if (committer is FileOutputCommitter)
			{
				workPath = ((FileOutputCommitter)committer).GetWorkPath(context, outputPath);
			}
			// ${mapred.out.dir}/_temporary/_${taskid}/${name}
			return new Path(workPath, name);
		}

		/// <summary>Helper function to generate a name that is unique for the task.</summary>
		/// <remarks>
		/// Helper function to generate a name that is unique for the task.
		/// <p>The generated name can be used to create custom files from within the
		/// different tasks for the job, the names for different tasks will not collide
		/// with each other.</p>
		/// <p>The given name is postfixed with the task type, 'm' for maps, 'r' for
		/// reduces and the task partition number. For example, give a name 'test'
		/// running on the first map o the job the generated name will be
		/// 'test-m-00000'.</p>
		/// </remarks>
		/// <param name="conf">the configuration for the job.</param>
		/// <param name="name">the name to make unique.</param>
		/// <returns>a unique name accross all tasks of the job.</returns>
		public static string GetUniqueName(JobConf conf, string name)
		{
			int partition = conf.GetInt(JobContext.TaskPartition, -1);
			if (partition == -1)
			{
				throw new ArgumentException("This method can only be called from within a Job");
			}
			string taskType = conf.GetBoolean(JobContext.TaskIsmap, JobContext.DefaultTaskIsmap
				) ? "m" : "r";
			NumberFormat numberFormat = NumberFormat.GetInstance();
			numberFormat.SetMinimumIntegerDigits(5);
			numberFormat.SetGroupingUsed(false);
			return name + "-" + taskType + "-" + numberFormat.Format(partition);
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
		/// <see cref="FileOutputFormat{K, V}.GetUniqueName(JobConf, string)"/>
		/// method to make the file name
		/// unique for the task.</p>
		/// </summary>
		/// <param name="conf">the configuration for the job.</param>
		/// <param name="name">the name for the file.</param>
		/// <returns>a unique path accross all tasks of the job.</returns>
		public static Path GetPathForCustomFile(JobConf conf, string name)
		{
			return new Path(GetWorkOutputPath(conf), GetUniqueName(conf, name));
		}
	}
}
