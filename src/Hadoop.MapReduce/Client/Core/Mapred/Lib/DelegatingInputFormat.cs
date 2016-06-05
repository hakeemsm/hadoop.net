using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Mapred.InputFormat{K, V}"/>
	/// that delegates behaviour of paths to multiple other
	/// InputFormats.
	/// </summary>
	/// <seealso cref="MultipleInputs.AddInputPath(Org.Apache.Hadoop.Mapred.JobConf, Org.Apache.Hadoop.FS.Path, System.Type{T}, System.Type{T})
	/// 	"/>
	public class DelegatingInputFormat<K, V> : InputFormat<K, V>
	{
		/// <exception cref="System.IO.IOException"/>
		public virtual InputSplit[] GetSplits(JobConf conf, int numSplits)
		{
			JobConf confCopy = new JobConf(conf);
			IList<InputSplit> splits = new AList<InputSplit>();
			IDictionary<Path, InputFormat> formatMap = MultipleInputs.GetInputFormatMap(conf);
			IDictionary<Path, Type> mapperMap = MultipleInputs.GetMapperTypeMap(conf);
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
						mapperClass = conf.GetMapperClass();
					}
					FileInputFormat.SetInputPaths(confCopy, Sharpen.Collections.ToArray(paths, new Path
						[paths.Count]));
					// Get splits for each input path and tag with InputFormat
					// and Mapper types by wrapping in a TaggedInputSplit.
					InputSplit[] pathSplits = format.GetSplits(confCopy, numSplits);
					foreach (InputSplit pathSplit in pathSplits)
					{
						splits.AddItem(new TaggedInputSplit(pathSplit, conf, format.GetType(), mapperClass
							));
					}
				}
			}
			return Sharpen.Collections.ToArray(splits, new InputSplit[splits.Count]);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual RecordReader<K, V> GetRecordReader(InputSplit split, JobConf conf, 
			Reporter reporter)
		{
			// Find the InputFormat and then the RecordReader from the
			// TaggedInputSplit.
			TaggedInputSplit taggedInputSplit = (TaggedInputSplit)split;
			InputFormat<K, V> inputFormat = (InputFormat<K, V>)ReflectionUtils.NewInstance(taggedInputSplit
				.GetInputFormatClass(), conf);
			return inputFormat.GetRecordReader(taggedInputSplit.GetInputSplit(), conf, reporter
				);
		}
	}
}
