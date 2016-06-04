using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Serializer;
using Org.Apache.Hadoop.Util;

namespace Hadoop.Common.Core.Util
{
	/// <summary>General reflection utils</summary>
	public class ReflectionUtils
	{
		private static readonly Type[] EmptyArray = new Type[] {  };

		private static volatile SerializationFactory serialFactory = null;

		/// <summary>Cache of constructors for each class.</summary>
		/// <remarks>
		/// Cache of constructors for each class. Pins the classes so they
		/// can't be garbage collected until ReflectionUtils can be collected.
		/// </remarks>
		private static readonly IDictionary<Type, Constructor<object>> ConstructorCache = 
			new ConcurrentHashMap<Type, Constructor<object>>();

		/// <summary>Check and set 'configuration' if necessary.</summary>
		/// <param name="theObject">object for which to set configuration</param>
		/// <param name="conf">Configuration</param>
		public static void SetConf(object theObject, Configuration conf)
		{
			if (conf != null)
			{
				if (theObject is Configurable)
				{
					((Configurable)theObject).SetConf(conf);
				}
				SetJobConf(theObject, conf);
			}
		}

		/// <summary>
		/// This code is to support backward compatibility and break the compile
		/// time dependency of core on mapred.
		/// </summary>
		/// <remarks>
		/// This code is to support backward compatibility and break the compile
		/// time dependency of core on mapred.
		/// This should be made deprecated along with the mapred package HADOOP-1230.
		/// Should be removed when mapred package is removed.
		/// </remarks>
		private static void SetJobConf(object theObject, Configuration conf)
		{
			//If JobConf and JobConfigurable are in classpath, AND
			//theObject is of type JobConfigurable AND
			//conf is of type JobConf then
			//invoke configure on theObject
			try
			{
				Type jobConfClass = conf.GetClassByNameOrNull("org.apache.hadoop.mapred.JobConf");
				if (jobConfClass == null)
				{
					return;
				}
				Type jobConfigurableClass = conf.GetClassByNameOrNull("org.apache.hadoop.mapred.JobConfigurable"
					);
				if (jobConfigurableClass == null)
				{
					return;
				}
				if (jobConfClass.IsAssignableFrom(conf.GetType()) && jobConfigurableClass.IsAssignableFrom
					(theObject.GetType()))
				{
					MethodInfo configureMethod = jobConfigurableClass.GetMethod("configure", jobConfClass
						);
					configureMethod.Invoke(theObject, conf);
				}
			}
			catch (Exception e)
			{
				throw new RuntimeException("Error in configuring object", e);
			}
		}

		/// <summary>Create an object for the given class and initialize it from conf</summary>
		/// <param name="theClass">class of which an object is created</param>
		/// <param name="conf">Configuration</param>
		/// <returns>a new object</returns>
		public static T NewInstance<T>(Configuration conf)
		{
			System.Type theClass = typeof(T);
			T result;
			try
			{
				Constructor<T> meth = (Constructor<T>)ConstructorCache[theClass];
				if (meth == null)
				{
					meth = theClass.GetDeclaredConstructor(EmptyArray);
					ConstructorCache[theClass] = meth;
				}
				result = meth.NewInstance();
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
			SetConf(result, conf);
			return result;
		}

		private static ThreadMXBean threadBean = ManagementFactory.GetThreadMXBean();

		public static void SetContentionTracing(bool val)
		{
			threadBean.SetThreadContentionMonitoringEnabled(val);
		}

		private static string GetTaskName(long id, string name)
		{
			if (name == null)
			{
				return System.Convert.ToString(id);
			}
			return id + " (" + name + ")";
		}

		/// <summary>Print all of the thread's information and stack traces.</summary>
		/// <param name="stream">the stream to</param>
		/// <param name="title">a string title for the stack trace</param>
		public static void PrintThreadInfo(TextWriter stream, string title)
		{
			lock (typeof(ReflectionUtils))
			{
				int StackDepth = 20;
				bool contention = threadBean.IsThreadContentionMonitoringEnabled();
				long[] threadIds = threadBean.GetAllThreadIds();
				stream.WriteLine("Process Thread Dump: " + title);
				stream.WriteLine(threadIds.Length + " active threads");
				foreach (long tid in threadIds)
				{
					ThreadInfo info = threadBean.GetThreadInfo(tid, StackDepth);
					if (info == null)
					{
						stream.WriteLine("  Inactive");
						continue;
					}
					stream.WriteLine("Thread " + GetTaskName(info.GetThreadId(), info.GetThreadName()
						) + ":");
					Sharpen.Thread.State state = info.GetThreadState();
					stream.WriteLine("  State: " + state);
					stream.WriteLine("  Blocked count: " + info.GetBlockedCount());
					stream.WriteLine("  Waited count: " + info.GetWaitedCount());
					if (contention)
					{
						stream.WriteLine("  Blocked time: " + info.GetBlockedTime());
						stream.WriteLine("  Waited time: " + info.GetWaitedTime());
					}
					if (state == Sharpen.Thread.State.Waiting)
					{
						stream.WriteLine("  Waiting on " + info.GetLockName());
					}
					else
					{
						if (state == Sharpen.Thread.State.Blocked)
						{
							stream.WriteLine("  Blocked on " + info.GetLockName());
							stream.WriteLine("  Blocked by " + GetTaskName(info.GetLockOwnerId(), info.GetLockOwnerName
								()));
						}
					}
					stream.WriteLine("  Stack:");
					foreach (StackTraceElement frame in info.GetStackTrace())
					{
						stream.WriteLine("    " + frame.ToString());
					}
				}
				stream.Flush();
			}
		}

		private static long previousLogTime = 0;

		/// <summary>Log the current thread stacks at INFO level.</summary>
		/// <param name="log">the logger that logs the stack trace</param>
		/// <param name="title">a descriptive title for the call stacks</param>
		/// <param name="minInterval">the minimum time from the last</param>
		public static void LogThreadInfo(Org.Apache.Hadoop.Log log, string title, long minInterval)
		{
			bool dumpStack = false;
			if (log.IsInfoEnabled())
			{
				lock (typeof(ReflectionUtils))
				{
					long now = Time.Now();
					if (now - previousLogTime >= minInterval * 1000)
					{
						previousLogTime = now;
						dumpStack = true;
					}
				}
				if (dumpStack)
				{
					try
					{
						ByteArrayOutputStream buffer = new ByteArrayOutputStream();
						PrintThreadInfo(new TextWriter(buffer, false, "UTF-8"), title);
						log.Info(buffer.ToString(Encoding.Default.Name()));
					}
					catch (UnsupportedEncodingException)
					{
					}
				}
			}
		}

		/// <summary>
		/// Return the correctly-typed
		/// <see cref="System.Type{T}"/>
		/// of the given object.
		/// </summary>
		/// <param name="o">object whose correctly-typed <code>Class</code> is to be obtained
		/// 	</param>
		/// <returns>the correctly typed <code>Class</code> of the given object.</returns>
		public static Type GetClass<T>(T o)
		{
			return (Type)o.GetType();
		}

		// methods to support testing
		internal static void ClearCache()
		{
			ConstructorCache.Clear();
		}

		internal static int GetCacheSize()
		{
			return ConstructorCache.Count;
		}

		/// <summary>A pair of input/output buffers that we use to clone writables.</summary>
		private class CopyInCopyOutBuffer
		{
			internal DataOutputBuffer outBuffer = new DataOutputBuffer();

			internal DataInputBuffer inBuffer = new DataInputBuffer();

			/// <summary>Move the data from the output buffer to the input buffer.</summary>
			internal virtual void MoveData()
			{
				inBuffer.Reset(outBuffer.GetData(), outBuffer.GetLength());
			}
		}

		private sealed class _ThreadLocal_268 : ThreadLocal<ReflectionUtils.CopyInCopyOutBuffer
			>
		{
			public _ThreadLocal_268()
			{
			}

			protected override ReflectionUtils.CopyInCopyOutBuffer InitialValue()
			{
				lock (this)
				{
					return new ReflectionUtils.CopyInCopyOutBuffer();
				}
			}
		}

		/// <summary>Allocate a buffer for each thread that tries to clone objects.</summary>
		private static ThreadLocal<ReflectionUtils.CopyInCopyOutBuffer> cloneBuffers = new 
			_ThreadLocal_268();

		private static SerializationFactory GetFactory(Configuration conf)
		{
			if (serialFactory == null)
			{
				serialFactory = new SerializationFactory(conf);
			}
			return serialFactory;
		}

		/// <summary>Make a copy of the writable object using serialization to a buffer</summary>
		/// <param name="src">the object to copy from</param>
		/// <param name="dst">the object to copy into, which is destroyed</param>
		/// <returns>dst param (the copy)</returns>
		/// <exception cref="System.IO.IOException"/>
		public static T Copy<T>(Configuration conf, T src, T dst)
		{
			ReflectionUtils.CopyInCopyOutBuffer buffer = cloneBuffers.Get();
			buffer.outBuffer.Reset();
			SerializationFactory factory = GetFactory(conf);
			Type cls = (Type)src.GetType();
			Org.Apache.Hadoop.IO.Serializer.Serializer<T> serializer = factory.GetSerializer(
				cls);
			serializer.Open(buffer.outBuffer);
			serializer.Serialize(src);
			buffer.MoveData();
			Deserializer<T> deserializer = factory.GetDeserializer(cls);
			deserializer.Open(buffer.inBuffer);
			dst = deserializer.Deserialize(dst);
			return dst;
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public static void CloneWritableInto(IWritable dst, IWritable src)
		{
			ReflectionUtils.CopyInCopyOutBuffer buffer = cloneBuffers.Get();
			buffer.outBuffer.Reset();
			src.Write(buffer.outBuffer);
			buffer.MoveData();
			dst.ReadFields(buffer.inBuffer);
		}

		/// <summary>
		/// Gets all the declared fields of a class including fields declared in
		/// superclasses.
		/// </summary>
		public static IList<FieldInfo> GetDeclaredFieldsIncludingInherited(Type clazz)
		{
			IList<FieldInfo> fields = new AList<FieldInfo>();
			while (clazz != null)
			{
				foreach (FieldInfo field in Sharpen.Runtime.GetDeclaredFields(clazz))
				{
					fields.AddItem(field);
				}
				clazz = clazz.BaseType;
			}
			return fields;
		}

		/// <summary>
		/// Gets all the declared methods of a class including methods declared in
		/// superclasses.
		/// </summary>
		public static IList<MethodInfo> GetDeclaredMethodsIncludingInherited(Type clazz)
		{
			IList<MethodInfo> methods = new AList<MethodInfo>();
			while (clazz != null)
			{
				foreach (MethodInfo method in Sharpen.Runtime.GetDeclaredMethods(clazz))
				{
					methods.AddItem(method);
				}
				clazz = clazz.BaseType;
			}
			return methods;
		}
	}
}
