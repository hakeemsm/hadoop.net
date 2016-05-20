using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>General reflection utils</summary>
	public class ReflectionUtils
	{
		private static readonly java.lang.Class[] EMPTY_ARRAY = new java.lang.Class[] {  };

		private static volatile org.apache.hadoop.io.serializer.SerializationFactory serialFactory
			 = null;

		/// <summary>Cache of constructors for each class.</summary>
		/// <remarks>
		/// Cache of constructors for each class. Pins the classes so they
		/// can't be garbage collected until ReflectionUtils can be collected.
		/// </remarks>
		private static readonly System.Collections.Generic.IDictionary<java.lang.Class, java.lang.reflect.Constructor
			<object>> CONSTRUCTOR_CACHE = new java.util.concurrent.ConcurrentHashMap<java.lang.Class
			, java.lang.reflect.Constructor<object>>();

		/// <summary>Check and set 'configuration' if necessary.</summary>
		/// <param name="theObject">object for which to set configuration</param>
		/// <param name="conf">Configuration</param>
		public static void setConf(object theObject, org.apache.hadoop.conf.Configuration
			 conf)
		{
			if (conf != null)
			{
				if (theObject is org.apache.hadoop.conf.Configurable)
				{
					((org.apache.hadoop.conf.Configurable)theObject).setConf(conf);
				}
				setJobConf(theObject, conf);
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
		private static void setJobConf(object theObject, org.apache.hadoop.conf.Configuration
			 conf)
		{
			//If JobConf and JobConfigurable are in classpath, AND
			//theObject is of type JobConfigurable AND
			//conf is of type JobConf then
			//invoke configure on theObject
			try
			{
				java.lang.Class jobConfClass = conf.getClassByNameOrNull("org.apache.hadoop.mapred.JobConf"
					);
				if (jobConfClass == null)
				{
					return;
				}
				java.lang.Class jobConfigurableClass = conf.getClassByNameOrNull("org.apache.hadoop.mapred.JobConfigurable"
					);
				if (jobConfigurableClass == null)
				{
					return;
				}
				if (jobConfClass.isAssignableFrom(Sharpen.Runtime.getClassForObject(conf)) && jobConfigurableClass
					.isAssignableFrom(Sharpen.Runtime.getClassForObject(theObject)))
				{
					java.lang.reflect.Method configureMethod = jobConfigurableClass.getMethod("configure"
						, jobConfClass);
					configureMethod.invoke(theObject, conf);
				}
			}
			catch (System.Exception e)
			{
				throw new System.Exception("Error in configuring object", e);
			}
		}

		/// <summary>Create an object for the given class and initialize it from conf</summary>
		/// <param name="theClass">class of which an object is created</param>
		/// <param name="conf">Configuration</param>
		/// <returns>a new object</returns>
		public static T newInstance<T>(org.apache.hadoop.conf.Configuration conf)
		{
			System.Type theClass = typeof(T);
			T result;
			try
			{
				java.lang.reflect.Constructor<T> meth = (java.lang.reflect.Constructor<T>)CONSTRUCTOR_CACHE
					[theClass];
				if (meth == null)
				{
					meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
					meth.setAccessible(true);
					CONSTRUCTOR_CACHE[theClass] = meth;
				}
				result = meth.newInstance();
			}
			catch (System.Exception e)
			{
				throw new System.Exception(e);
			}
			setConf(result, conf);
			return result;
		}

		private static java.lang.management.ThreadMXBean threadBean = java.lang.management.ManagementFactory
			.getThreadMXBean();

		public static void setContentionTracing(bool val)
		{
			threadBean.setThreadContentionMonitoringEnabled(val);
		}

		private static string getTaskName(long id, string name)
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
		public static void printThreadInfo(System.IO.TextWriter stream, string title)
		{
			lock (typeof(ReflectionUtils))
			{
				int STACK_DEPTH = 20;
				bool contention = threadBean.isThreadContentionMonitoringEnabled();
				long[] threadIds = threadBean.getAllThreadIds();
				stream.WriteLine("Process Thread Dump: " + title);
				stream.WriteLine(threadIds.Length + " active threads");
				foreach (long tid in threadIds)
				{
					java.lang.management.ThreadInfo info = threadBean.getThreadInfo(tid, STACK_DEPTH);
					if (info == null)
					{
						stream.WriteLine("  Inactive");
						continue;
					}
					stream.WriteLine("Thread " + getTaskName(info.getThreadId(), info.getThreadName()
						) + ":");
					java.lang.Thread.State state = info.getThreadState();
					stream.WriteLine("  State: " + state);
					stream.WriteLine("  Blocked count: " + info.getBlockedCount());
					stream.WriteLine("  Waited count: " + info.getWaitedCount());
					if (contention)
					{
						stream.WriteLine("  Blocked time: " + info.getBlockedTime());
						stream.WriteLine("  Waited time: " + info.getWaitedTime());
					}
					if (state == java.lang.Thread.State.WAITING)
					{
						stream.WriteLine("  Waiting on " + info.getLockName());
					}
					else
					{
						if (state == java.lang.Thread.State.BLOCKED)
						{
							stream.WriteLine("  Blocked on " + info.getLockName());
							stream.WriteLine("  Blocked by " + getTaskName(info.getLockOwnerId(), info.getLockOwnerName
								()));
						}
					}
					stream.WriteLine("  Stack:");
					foreach (java.lang.StackTraceElement frame in info.getStackTrace())
					{
						stream.WriteLine("    " + frame.ToString());
					}
				}
				stream.flush();
			}
		}

		private static long previousLogTime = 0;

		/// <summary>Log the current thread stacks at INFO level.</summary>
		/// <param name="log">the logger that logs the stack trace</param>
		/// <param name="title">a descriptive title for the call stacks</param>
		/// <param name="minInterval">the minimum time from the last</param>
		public static void logThreadInfo(org.apache.commons.logging.Log log, string title
			, long minInterval)
		{
			bool dumpStack = false;
			if (log.isInfoEnabled())
			{
				lock (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.ReflectionUtils
					)))
				{
					long now = org.apache.hadoop.util.Time.now();
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
						java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
						printThreadInfo(new System.IO.TextWriter(buffer, false, "UTF-8"), title);
						log.info(buffer.toString(java.nio.charset.Charset.defaultCharset().name()));
					}
					catch (java.io.UnsupportedEncodingException)
					{
					}
				}
			}
		}

		/// <summary>
		/// Return the correctly-typed
		/// <see cref="java.lang.Class{T}"/>
		/// of the given object.
		/// </summary>
		/// <param name="o">object whose correctly-typed <code>Class</code> is to be obtained
		/// 	</param>
		/// <returns>the correctly typed <code>Class</code> of the given object.</returns>
		public static java.lang.Class getClass<T>(T o)
		{
			return (java.lang.Class)Sharpen.Runtime.getClassForObject(o);
		}

		// methods to support testing
		internal static void clearCache()
		{
			CONSTRUCTOR_CACHE.clear();
		}

		internal static int getCacheSize()
		{
			return CONSTRUCTOR_CACHE.Count;
		}

		/// <summary>A pair of input/output buffers that we use to clone writables.</summary>
		private class CopyInCopyOutBuffer
		{
			internal org.apache.hadoop.io.DataOutputBuffer outBuffer = new org.apache.hadoop.io.DataOutputBuffer
				();

			internal org.apache.hadoop.io.DataInputBuffer inBuffer = new org.apache.hadoop.io.DataInputBuffer
				();

			/// <summary>Move the data from the output buffer to the input buffer.</summary>
			internal virtual void moveData()
			{
				inBuffer.reset(outBuffer.getData(), outBuffer.getLength());
			}
		}

		private sealed class _ThreadLocal_268 : java.lang.ThreadLocal<org.apache.hadoop.util.ReflectionUtils.CopyInCopyOutBuffer
			>
		{
			public _ThreadLocal_268()
			{
			}

			protected override org.apache.hadoop.util.ReflectionUtils.CopyInCopyOutBuffer initialValue
				()
			{
				lock (this)
				{
					return new org.apache.hadoop.util.ReflectionUtils.CopyInCopyOutBuffer();
				}
			}
		}

		/// <summary>Allocate a buffer for each thread that tries to clone objects.</summary>
		private static java.lang.ThreadLocal<org.apache.hadoop.util.ReflectionUtils.CopyInCopyOutBuffer
			> cloneBuffers = new _ThreadLocal_268();

		private static org.apache.hadoop.io.serializer.SerializationFactory getFactory(org.apache.hadoop.conf.Configuration
			 conf)
		{
			if (serialFactory == null)
			{
				serialFactory = new org.apache.hadoop.io.serializer.SerializationFactory(conf);
			}
			return serialFactory;
		}

		/// <summary>Make a copy of the writable object using serialization to a buffer</summary>
		/// <param name="src">the object to copy from</param>
		/// <param name="dst">the object to copy into, which is destroyed</param>
		/// <returns>dst param (the copy)</returns>
		/// <exception cref="System.IO.IOException"/>
		public static T copy<T>(org.apache.hadoop.conf.Configuration conf, T src, T dst)
		{
			org.apache.hadoop.util.ReflectionUtils.CopyInCopyOutBuffer buffer = cloneBuffers.
				get();
			buffer.outBuffer.reset();
			org.apache.hadoop.io.serializer.SerializationFactory factory = getFactory(conf);
			java.lang.Class cls = (java.lang.Class)Sharpen.Runtime.getClassForObject(src);
			org.apache.hadoop.io.serializer.Serializer<T> serializer = factory.getSerializer(
				cls);
			serializer.open(buffer.outBuffer);
			serializer.serialize(src);
			buffer.moveData();
			org.apache.hadoop.io.serializer.Deserializer<T> deserializer = factory.getDeserializer
				(cls);
			deserializer.open(buffer.inBuffer);
			dst = deserializer.deserialize(dst);
			return dst;
		}

		/// <exception cref="System.IO.IOException"/>
		[System.Obsolete]
		public static void cloneWritableInto(org.apache.hadoop.io.Writable dst, org.apache.hadoop.io.Writable
			 src)
		{
			org.apache.hadoop.util.ReflectionUtils.CopyInCopyOutBuffer buffer = cloneBuffers.
				get();
			buffer.outBuffer.reset();
			src.write(buffer.outBuffer);
			buffer.moveData();
			dst.readFields(buffer.inBuffer);
		}

		/// <summary>
		/// Gets all the declared fields of a class including fields declared in
		/// superclasses.
		/// </summary>
		public static System.Collections.Generic.IList<java.lang.reflect.Field> getDeclaredFieldsIncludingInherited
			(java.lang.Class clazz)
		{
			System.Collections.Generic.IList<java.lang.reflect.Field> fields = new System.Collections.Generic.List
				<java.lang.reflect.Field>();
			while (clazz != null)
			{
				foreach (java.lang.reflect.Field field in clazz.getDeclaredFields())
				{
					fields.add(field);
				}
				clazz = clazz.getSuperclass();
			}
			return fields;
		}

		/// <summary>
		/// Gets all the declared methods of a class including methods declared in
		/// superclasses.
		/// </summary>
		public static System.Collections.Generic.IList<java.lang.reflect.Method> getDeclaredMethodsIncludingInherited
			(java.lang.Class clazz)
		{
			System.Collections.Generic.IList<java.lang.reflect.Method> methods = new System.Collections.Generic.List
				<java.lang.reflect.Method>();
			while (clazz != null)
			{
				foreach (java.lang.reflect.Method method in clazz.getDeclaredMethods())
				{
					methods.add(method);
				}
				clazz = clazz.getSuperclass();
			}
			return methods;
		}
	}
}
