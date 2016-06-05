using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// This class supports MapReduce jobs that have multiple input paths with
	/// a different
	/// <see cref="Org.Apache.Hadoop.Mapred.InputFormat{K, V}"/>
	/// and
	/// <see cref="Org.Apache.Hadoop.Mapred.Mapper{K1, V1, K2, V2}"/>
	/// for each path
	/// </summary>
	public class MultipleInputs
	{
		/// <summary>
		/// Add a
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// with a custom
		/// <see cref="Org.Apache.Hadoop.Mapred.InputFormat{K, V}"/>
		/// to the list of
		/// inputs for the map-reduce job.
		/// </summary>
		/// <param name="conf">The configuration of the job</param>
		/// <param name="path">
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// to be added to the list of inputs for the job
		/// </param>
		/// <param name="inputFormatClass">
		/// 
		/// <see cref="Org.Apache.Hadoop.Mapred.InputFormat{K, V}"/>
		/// class to use for this path
		/// </param>
		public static void AddInputPath(JobConf conf, Path path, Type inputFormatClass)
		{
			string inputFormatMapping = path.ToString() + ";" + inputFormatClass.FullName;
			string inputFormats = conf.Get("mapreduce.input.multipleinputs.dir.formats");
			conf.Set("mapreduce.input.multipleinputs.dir.formats", inputFormats == null ? inputFormatMapping
				 : inputFormats + "," + inputFormatMapping);
			conf.SetInputFormat(typeof(DelegatingInputFormat));
		}

		/// <summary>
		/// Add a
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// with a custom
		/// <see cref="Org.Apache.Hadoop.Mapred.InputFormat{K, V}"/>
		/// and
		/// <see cref="Org.Apache.Hadoop.Mapred.Mapper{K1, V1, K2, V2}"/>
		/// to the list of inputs for the map-reduce job.
		/// </summary>
		/// <param name="conf">The configuration of the job</param>
		/// <param name="path">
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// to be added to the list of inputs for the job
		/// </param>
		/// <param name="inputFormatClass">
		/// 
		/// <see cref="Org.Apache.Hadoop.Mapred.InputFormat{K, V}"/>
		/// class to use for this path
		/// </param>
		/// <param name="mapperClass">
		/// 
		/// <see cref="Org.Apache.Hadoop.Mapred.Mapper{K1, V1, K2, V2}"/>
		/// class to use for this path
		/// </param>
		public static void AddInputPath(JobConf conf, Path path, Type inputFormatClass, Type
			 mapperClass)
		{
			AddInputPath(conf, path, inputFormatClass);
			string mapperMapping = path.ToString() + ";" + mapperClass.FullName;
			string mappers = conf.Get("mapreduce.input.multipleinputs.dir.mappers");
			conf.Set("mapreduce.input.multipleinputs.dir.mappers", mappers == null ? mapperMapping
				 : mappers + "," + mapperMapping);
			conf.SetMapperClass(typeof(DelegatingMapper));
		}

		/// <summary>
		/// Retrieves a map of
		/// <see cref="Org.Apache.Hadoop.FS.Path"/>
		/// s to the
		/// <see cref="Org.Apache.Hadoop.Mapred.InputFormat{K, V}"/>
		/// class
		/// that should be used for them.
		/// </summary>
		/// <param name="conf">The confuration of the job</param>
		/// <seealso cref="AddInputPath(Org.Apache.Hadoop.Mapred.JobConf, Org.Apache.Hadoop.FS.Path, System.Type{T})
		/// 	"/>
		/// <returns>A map of paths to inputformats for the job</returns>
		internal static IDictionary<Path, InputFormat> GetInputFormatMap(JobConf conf)
		{
			IDictionary<Path, InputFormat> m = new Dictionary<Path, InputFormat>();
			string[] pathMappings = conf.Get("mapreduce.input.multipleinputs.dir.formats").Split
				(",");
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
		/// <see cref="Org.Apache.Hadoop.Mapred.Mapper{K1, V1, K2, V2}"/>
		/// class that
		/// should be used for them.
		/// </summary>
		/// <param name="conf">The confuration of the job</param>
		/// <seealso cref="AddInputPath(Org.Apache.Hadoop.Mapred.JobConf, Org.Apache.Hadoop.FS.Path, System.Type{T}, System.Type{T})
		/// 	"/>
		/// <returns>A map of paths to mappers for the job</returns>
		internal static IDictionary<Path, Type> GetMapperTypeMap(JobConf conf)
		{
			if (conf.Get("mapreduce.input.multipleinputs.dir.mappers") == null)
			{
				return Sharpen.Collections.EmptyMap();
			}
			IDictionary<Path, Type> m = new Dictionary<Path, Type>();
			string[] pathMappings = conf.Get("mapreduce.input.multipleinputs.dir.mappers").Split
				(",");
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
