using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}"/>
	/// that delegates behavior of paths to multiple other
	/// InputFormats.
	/// </summary>
	/// <seealso cref="MultipleInputs.AddInputPath(Org.Apache.Hadoop.Mapreduce.Job, Org.Apache.Hadoop.FS.Path, System.Type{T}, System.Type{T})
	/// 	"/>
	public class DelegatingInputFormat<K, V> : InputFormat<K, V>
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override IList<InputSplit> GetSplits(JobContext job)
		{
			Configuration conf = job.GetConfiguration();
			Job jobCopy = Job.GetInstance(conf);
			IList<InputSplit> splits = new AList<InputSplit>();
			IDictionary<Path, InputFormat> formatMap = MultipleInputs.GetInputFormatMap(job);
			IDictionary<Path, Type> mapperMap = MultipleInputs.GetMapperTypeMap(job);
			IDictionary<Type, IList<Path>> formatPaths = new Dictionary<Type, IList<Path>>();
			// First, build a map of InputFormats to Paths
			foreach (KeyValuePair<Path, InputFormat> entry in formatMap)
			{
				if (!formatPaths.Contains(entry.Value.GetType()))
				{
					formatPaths[entry.Value.GetType()] = new List<Path>();
				}
				formatPaths[entry.Value.GetType()].AddItem(entry.Key);
			}
			foreach (KeyValuePair<Type, IList<Path>> formatEntry in formatPaths)
			{
				Type formatClass = formatEntry.Key;
				InputFormat format = (InputFormat)ReflectionUtils.NewInstance(formatClass, conf);
				IList<Path> paths = formatEntry.Value;
				IDictionary<Type, IList<Path>> mapperPaths = new Dictionary<Type, IList<Path>>();
				// Now, for each set of paths that have a common InputFormat, build
				// a map of Mappers to the paths they're used for
				foreach (Path path in paths)
				{
					Type mapperClass = mapperMap[path];
					if (!mapperPaths.Contains(mapperClass))
					{
						mapperPaths[mapperClass] = new List<Path>();
					}
					mapperPaths[mapperClass].AddItem(path);
				}
				// Now each set of paths that has a common InputFormat and Mapper can
				// be added to the same job, and split together.
				foreach (KeyValuePair<Type, IList<Path>> mapEntry in mapperPaths)
				{
					paths = mapEntry.Value;
					Type mapperClass = mapEntry.Key;
					if (mapperClass == null)
					{
						try
						{
							mapperClass = job.GetMapperClass();
						}
						catch (TypeLoadException e)
						{
							throw new IOException("Mapper class is not found", e);
						}
					}
					FileInputFormat.SetInputPaths(jobCopy, Sharpen.Collections.ToArray(paths, new Path
						[paths.Count]));
					// Get splits for each input path and tag with InputFormat
					// and Mapper types by wrapping in a TaggedInputSplit.
					IList<InputSplit> pathSplits = format.GetSplits(jobCopy);
					foreach (InputSplit pathSplit in pathSplits)
					{
						splits.AddItem(new TaggedInputSplit(pathSplit, conf, format.GetType(), mapperClass
							));
					}
				}
			}
			return splits;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override RecordReader<K, V> CreateRecordReader(InputSplit split, TaskAttemptContext
			 context)
		{
			return new DelegatingRecordReader<K, V>(split, context);
		}
	}
}
