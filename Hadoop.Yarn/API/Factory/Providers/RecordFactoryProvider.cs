using System;
using System.Reflection;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Factory.Providers
{
	public class RecordFactoryProvider
	{
		private static Configuration defaultConf;

		static RecordFactoryProvider()
		{
			defaultConf = new Configuration();
		}

		private RecordFactoryProvider()
		{
		}

		public static RecordFactory GetRecordFactory(Configuration conf)
		{
			if (conf == null)
			{
				//Assuming the default configuration has the correct factories set.
				//Users can specify a particular factory by providing a configuration.
				conf = defaultConf;
			}
			string recordFactoryClassName = conf.Get(YarnConfiguration.IpcRecordFactoryClass, 
				YarnConfiguration.DefaultIpcRecordFactoryClass);
			return (RecordFactory)GetFactoryClassInstance(recordFactoryClassName);
		}

		private static object GetFactoryClassInstance(string factoryClassName)
		{
			try
			{
				Type clazz = Sharpen.Runtime.GetType(factoryClassName);
				MethodInfo method = clazz.GetMethod("get", null);
				return method.Invoke(null, null);
			}
			catch (TypeLoadException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (MissingMethodException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (TargetInvocationException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (MemberAccessException e)
			{
				throw new YarnRuntimeException(e);
			}
		}
	}
}
