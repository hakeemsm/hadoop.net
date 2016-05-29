using System;
using System.Reflection;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Yarn.Factories.Impl.PB
{
	public class RecordFactoryPBImpl : RecordFactory
	{
		private const string PbImplPackageSuffix = "impl.pb";

		private const string PbImplClassSuffix = "PBImpl";

		private static readonly Org.Apache.Hadoop.Yarn.Factories.Impl.PB.RecordFactoryPBImpl
			 self = new Org.Apache.Hadoop.Yarn.Factories.Impl.PB.RecordFactoryPBImpl();

		private Configuration localConf = new Configuration();

		private ConcurrentMap<Type, Constructor<object>> cache = new ConcurrentHashMap<Type
			, Constructor<object>>();

		private RecordFactoryPBImpl()
		{
		}

		public static RecordFactory Get()
		{
			return self;
		}

		public virtual T NewRecordInstance<T>()
		{
			System.Type clazz = typeof(T);
			Constructor<object> constructor = cache[clazz];
			if (constructor == null)
			{
				Type pbClazz = null;
				try
				{
					pbClazz = localConf.GetClassByName(GetPBImplClassName(clazz));
				}
				catch (TypeLoadException e)
				{
					throw new YarnRuntimeException("Failed to load class: [" + GetPBImplClassName(clazz
						) + "]", e);
				}
				try
				{
					constructor = pbClazz.GetConstructor();
					cache.PutIfAbsent(clazz, constructor);
				}
				catch (MissingMethodException e)
				{
					throw new YarnRuntimeException("Could not find 0 argument constructor", e);
				}
			}
			try
			{
				object retObject = constructor.NewInstance();
				return (T)retObject;
			}
			catch (TargetInvocationException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (MemberAccessException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (InstantiationException e)
			{
				throw new YarnRuntimeException(e);
			}
		}

		private string GetPBImplClassName(Type clazz)
		{
			string srcPackagePart = GetPackageName(clazz);
			string srcClassName = GetClassName(clazz);
			string destPackagePart = srcPackagePart + "." + PbImplPackageSuffix;
			string destClassPart = srcClassName + PbImplClassSuffix;
			return destPackagePart + "." + destClassPart;
		}

		private string GetClassName(Type clazz)
		{
			string fqName = clazz.FullName;
			return (Sharpen.Runtime.Substring(fqName, fqName.LastIndexOf(".") + 1, fqName.Length
				));
		}

		private string GetPackageName(Type clazz)
		{
			return clazz.Assembly.GetName();
		}
	}
}
