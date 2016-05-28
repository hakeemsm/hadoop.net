using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	/// <summary>
	/// This class supports MapReduce jobs that have multiple input paths with
	/// a different
	/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}"/>
	/// and
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
	/// 	>
	/// for each path
	/// </summary>
	public class MultipleInputs
	{
		public const string DirFormats = "mapreduce.input.multipleinputs.dir.formats";

		public const string DirMappers = "mapreduce.input.multipleinputs.dir.mappers";

		/// <summary>
		/// Add a
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// with a custom
		/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}"/>
		/// to the list of
		/// inputs for the map-reduce job.
		/// </summary>
		/// <param name="job">
		/// The
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Job"/>
		/// </param>
		/// <param name="path">
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// to be added to the list of inputs for the job
		/// </param>
		/// <param name="inputFormatClass">
		/// 
		/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}"/>
		/// class to use for this path
		/// </param>
		public static void AddInputPath(Job job, Path path, Type inputFormatClass)
		{
			string inputFormatMapping = path.ToString() + ";" + inputFormatClass.FullName;
			Configuration conf = job.GetConfiguration();
			string inputFormats = conf.Get(DirFormats);
			conf.Set(DirFormats, inputFormats == null ? inputFormatMapping : inputFormats + ","
				 + inputFormatMapping);
			job.SetInputFormatClass(typeof(DelegatingInputFormat));
		}

		/// <summary>
		/// Add a
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// with a custom
		/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}"/>
		/// and
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
		/// 	>
		/// to the list of inputs for the map-reduce job.
		/// </summary>
		/// <param name="job">
		/// The
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Job"/>
		/// </param>
		/// <param name="path">
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// to be added to the list of inputs for the job
		/// </param>
		/// <param name="inputFormatClass">
		/// 
		/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}"/>
		/// class to use for this path
		/// </param>
		/// <param name="mapperClass">
		/// 
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
		/// 	>
		/// class to use for this path
		/// </param>
		public static void AddInputPath(Job job, Path path, Type inputFormatClass, Type mapperClass
			)
		{
			AddInputPath(job, path, inputFormatClass);
			Configuration conf = job.GetConfiguration();
			string mapperMapping = path.ToString() + ";" + mapperClass.FullName;
			string mappers = conf.Get(DirMappers);
			conf.Set(DirMappers, mappers == null ? mapperMapping : mappers + "," + mapperMapping
				);
			job.SetMapperClass(typeof(DelegatingMapper));
		}

		/// <summary>
		/// Retrieves a map of
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// s to the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}"/>
		/// class
		/// that should be used for them.
		/// </summary>
		/// <param name="job">
		/// The
		/// <see cref="Org.Apache.Hadoop.Mapreduce.JobContext"/>
		/// </param>
		/// <seealso cref="#addInputPath(JobConf,Path,Class)"/>
		/// <returns>A map of paths to inputformats for the job</returns>
		internal static IDictionary<Path, InputFormat> GetInputFormatMap(JobContext job)
		{
			IDictionary<Path, InputFormat> m = new Dictionary<Path, InputFormat>();
			Configuration conf = job.GetConfiguration();
			string[] pathMappings = conf.Get(DirFormats).Split(",");
			foreach (string pathMapping in pathMappings)
			{
				string[] split = pathMapping.Split(";");
				InputFormat inputFormat;
				try
				{
					inputFormat = (InputFormat)ReflectionUtils.NewInstance(conf.GetClassByName(split[
						1]), conf);
				}
				catch (TypeLoadException e)
				{
					throw new RuntimeException(e);
				}
				m[new Path(split[0])] = inputFormat;
			}
			return m;
		}

		/// <summary>
		/// Retrieves a map of
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// s to the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
		/// 	>
		/// class that
		/// should be used for them.
		/// </summary>
		/// <param name="job">
		/// The
		/// <see cref="Org.Apache.Hadoop.Mapreduce.JobContext"/>
		/// </param>
		/// <seealso cref="#addInputPath(JobConf,Path,Class,Class)"/>
		/// <returns>A map of paths to mappers for the job</returns>
		internal static IDictionary<Path, Type> GetMapperTypeMap(JobContext job)
		{
			Configuration conf = job.GetConfiguration();
			if (conf.Get(DirMappers) == null)
			{
				return Sharpen.Collections.EmptyMap();
			}
			IDictionary<Path, Type> m = new Dictionary<Path, Type>();
			string[] pathMappings = conf.Get(DirMappers).Split(",");
			foreach (string pathMapping in pathMappings)
			{
				string[] split = pathMapping.Split(";");
				Type mapClass;
				try
				{
					mapClass = (Type)conf.GetClassByName(split[1]);
				}
				catch (TypeLoadException e)
				{
					throw new RuntimeException(e);
				}
				m[new Path(split[0])] = mapClass;
			}
			return m;
		}
	}
}
