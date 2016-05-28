using System;
using System.Reflection;
using System.Security;
using Org.Apache.Hadoop.Conf;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// A factory to allow applications to deal with inconsistencies between
	/// MapReduce Context Objects API between hadoop-0.20 and later versions.
	/// </summary>
	public class ContextFactory
	{
		private static readonly Constructor<object> JobContextConstructor;

		private static readonly Constructor<object> TaskContextConstructor;

		private static readonly Constructor<object> MapContextConstructor;

		private static readonly Constructor<object> MapContextImplConstructor;

		private static readonly bool useV21;

		private static readonly FieldInfo ReporterField;

		private static readonly FieldInfo ReaderField;

		private static readonly FieldInfo WriterField;

		private static readonly FieldInfo OuterMapField;

		private static readonly FieldInfo WrappedContextField;

		static ContextFactory()
		{
			bool v21 = true;
			string Package = "org.apache.hadoop.mapreduce";
			try
			{
				Sharpen.Runtime.GetType(Package + ".task.JobContextImpl");
			}
			catch (TypeLoadException)
			{
				v21 = false;
			}
			useV21 = v21;
			Type jobContextCls;
			Type taskContextCls;
			Type taskIOContextCls;
			Type mapCls;
			Type mapContextCls;
			Type innerMapContextCls;
			try
			{
				if (v21)
				{
					jobContextCls = Sharpen.Runtime.GetType(Package + ".task.JobContextImpl");
					taskContextCls = Sharpen.Runtime.GetType(Package + ".task.TaskAttemptContextImpl"
						);
					taskIOContextCls = Sharpen.Runtime.GetType(Package + ".task.TaskInputOutputContextImpl"
						);
					mapContextCls = Sharpen.Runtime.GetType(Package + ".task.MapContextImpl");
					mapCls = Sharpen.Runtime.GetType(Package + ".lib.map.WrappedMapper");
					innerMapContextCls = Sharpen.Runtime.GetType(Package + ".lib.map.WrappedMapper$Context"
						);
				}
				else
				{
					jobContextCls = Sharpen.Runtime.GetType(Package + ".JobContext");
					taskContextCls = Sharpen.Runtime.GetType(Package + ".TaskAttemptContext");
					taskIOContextCls = Sharpen.Runtime.GetType(Package + ".TaskInputOutputContext");
					mapContextCls = Sharpen.Runtime.GetType(Package + ".MapContext");
					mapCls = Sharpen.Runtime.GetType(Package + ".Mapper");
					innerMapContextCls = Sharpen.Runtime.GetType(Package + ".Mapper$Context");
				}
			}
			catch (TypeLoadException e)
			{
				throw new ArgumentException("Can't find class", e);
			}
			try
			{
				JobContextConstructor = jobContextCls.GetConstructor(typeof(Configuration), typeof(
					JobID));
				TaskContextConstructor = taskContextCls.GetConstructor(typeof(Configuration), typeof(
					TaskAttemptID));
				if (useV21)
				{
					MapContextConstructor = innerMapContextCls.GetConstructor(mapCls, typeof(MapContext
						));
					MapContextImplConstructor = mapContextCls.GetDeclaredConstructor(typeof(Configuration
						), typeof(TaskAttemptID), typeof(RecordReader), typeof(RecordWriter), typeof(OutputCommitter
						), typeof(StatusReporter), typeof(InputSplit));
					WrappedContextField = Sharpen.Runtime.GetDeclaredField(innerMapContextCls, "mapContext"
						);
				}
				else
				{
					MapContextConstructor = innerMapContextCls.GetConstructor(mapCls, typeof(Configuration
						), typeof(TaskAttemptID), typeof(RecordReader), typeof(RecordWriter), typeof(OutputCommitter
						), typeof(StatusReporter), typeof(InputSplit));
					MapContextImplConstructor = null;
					WrappedContextField = null;
				}
				ReporterField = Sharpen.Runtime.GetDeclaredField(taskContextCls, "reporter");
				ReaderField = Sharpen.Runtime.GetDeclaredField(mapContextCls, "reader");
				WriterField = Sharpen.Runtime.GetDeclaredField(taskIOContextCls, "output");
				OuterMapField = Sharpen.Runtime.GetDeclaredField(innerMapContextCls, "this$0");
			}
			catch (SecurityException e)
			{
				throw new ArgumentException("Can't run constructor ", e);
			}
			catch (MissingMethodException e)
			{
				throw new ArgumentException("Can't find constructor ", e);
			}
			catch (NoSuchFieldException e)
			{
				throw new ArgumentException("Can't find field ", e);
			}
		}

		/// <summary>
		/// Clone a
		/// <see cref="JobContext"/>
		/// or
		/// <see cref="TaskAttemptContext"/>
		/// with a
		/// new configuration.
		/// </summary>
		/// <param name="original">the original context</param>
		/// <param name="conf">the new configuration</param>
		/// <returns>a new context object</returns>
		/// <exception cref="System.Exception"></exception>
		/// <exception cref="System.IO.IOException"></exception>
		public static JobContext CloneContext(JobContext original, Configuration conf)
		{
			try
			{
				if (original is MapContext<object, object, object, object>)
				{
					return CloneMapContext((Mapper.Context)original, conf, null, null);
				}
				else
				{
					if (original is ReduceContext<object, object, object, object>)
					{
						throw new ArgumentException("can't clone ReduceContext");
					}
					else
					{
						if (original is TaskAttemptContext)
						{
							TaskAttemptContext spec = (TaskAttemptContext)original;
							return (JobContext)TaskContextConstructor.NewInstance(conf, spec.GetTaskAttemptID
								());
						}
						else
						{
							return (JobContext)JobContextConstructor.NewInstance(conf, original.GetJobID());
						}
					}
				}
			}
			catch (InstantiationException e)
			{
				throw new ArgumentException("Can't clone object", e);
			}
			catch (MemberAccessException e)
			{
				throw new ArgumentException("Can't clone object", e);
			}
			catch (TargetInvocationException e)
			{
				throw new ArgumentException("Can't clone object", e);
			}
		}

		/// <summary>
		/// Copy a custom WrappedMapper.Context, optionally replacing
		/// the input and output.
		/// </summary>
		/// <?/>
		/// <?/>
		/// <?/>
		/// <?/>
		/// <param name="context">the context to clone</param>
		/// <param name="conf">a new configuration</param>
		/// <param name="reader">Reader to read from. Null means to clone from context.</param>
		/// <param name="writer">Writer to write to. Null means to clone from context.</param>
		/// <returns>a new context. it will not be the same class as the original.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static Mapper.Context CloneMapContext<K1, V1, K2, V2>(MapContext<K1, V1, K2
			, V2> context, Configuration conf, RecordReader<K1, V1> reader, RecordWriter<K2, 
			V2> writer)
		{
			try
			{
				// get the outer object pointer
				object outer = OuterMapField.GetValue(context);
				// if it is a wrapped 21 context, unwrap it
				if ("org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context".Equals(context.GetType
					().FullName))
				{
					context = (MapContext<K1, V1, K2, V2>)WrappedContextField.GetValue(context);
				}
				// if the reader or writer aren't given, use the same ones
				if (reader == null)
				{
					reader = (RecordReader<K1, V1>)ReaderField.GetValue(context);
				}
				if (writer == null)
				{
					writer = (RecordWriter<K2, V2>)WriterField.GetValue(context);
				}
				if (useV21)
				{
					object basis = MapContextImplConstructor.NewInstance(conf, context.GetTaskAttemptID
						(), reader, writer, context.GetOutputCommitter(), ReporterField.GetValue(context
						), context.GetInputSplit());
					return (Mapper.Context)MapContextConstructor.NewInstance(outer, basis);
				}
				else
				{
					return (Mapper.Context)MapContextConstructor.NewInstance(outer, conf, context.GetTaskAttemptID
						(), reader, writer, context.GetOutputCommitter(), ReporterField.GetValue(context
						), context.GetInputSplit());
				}
			}
			catch (MemberAccessException e)
			{
				throw new ArgumentException("Can't access field", e);
			}
			catch (InstantiationException e)
			{
				throw new ArgumentException("Can't create object", e);
			}
			catch (TargetInvocationException e)
			{
				throw new ArgumentException("Can't invoke constructor", e);
			}
		}
	}
}
