using System;
using System.IO;
using System.Reflection;
using System.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class SerializedExceptionPBImpl : SerializedException
	{
		internal YarnProtos.SerializedExceptionProto proto = null;

		internal YarnProtos.SerializedExceptionProto.Builder builder = YarnProtos.SerializedExceptionProto
			.NewBuilder();

		internal bool viaProto = false;

		public SerializedExceptionPBImpl()
		{
		}

		public SerializedExceptionPBImpl(YarnProtos.SerializedExceptionProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		private SerializedExceptionPBImpl(Exception t)
		{
			Init(t);
		}

		public override void Init(string message)
		{
			MaybeInitBuilder();
			builder.SetMessage(message);
		}

		public override void Init(Exception t)
		{
			MaybeInitBuilder();
			if (t == null)
			{
				return;
			}
			if (t.InnerException == null)
			{
			}
			else
			{
				builder.SetCause(new Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB.SerializedExceptionPBImpl
					(t.InnerException).GetProto());
			}
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			Sharpen.Runtime.PrintStackTrace(t, pw);
			pw.Close();
			if (sw.ToString() != null)
			{
				builder.SetTrace(sw.ToString());
			}
			if (t.Message != null)
			{
				builder.SetMessage(t.Message);
			}
			builder.SetClassName(t.GetType().GetCanonicalName());
		}

		public override void Init(string message, Exception t)
		{
			Init(t);
			if (message != null)
			{
				builder.SetMessage(message);
			}
		}

		public override Exception DeSerialize()
		{
			SerializedException cause = GetCause();
			YarnProtos.SerializedExceptionProtoOrBuilder p = viaProto ? proto : builder;
			Type realClass = null;
			try
			{
				realClass = Sharpen.Runtime.GetType(p.GetClassName());
			}
			catch (TypeLoadException e)
			{
				throw new YarnRuntimeException(e);
			}
			Type classType = null;
			if (typeof(YarnException).IsAssignableFrom(realClass))
			{
				classType = typeof(YarnException);
			}
			else
			{
				if (typeof(IOException).IsAssignableFrom(realClass))
				{
					classType = typeof(IOException);
				}
				else
				{
					if (typeof(RuntimeException).IsAssignableFrom(realClass))
					{
						classType = typeof(RuntimeException);
					}
					else
					{
						classType = typeof(Exception);
					}
				}
			}
			return InstantiateException(realClass.AsSubclass(classType), GetMessage(), cause 
				== null ? null : cause.DeSerialize());
		}

		public override string GetMessage()
		{
			YarnProtos.SerializedExceptionProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetMessage();
		}

		public override string GetRemoteTrace()
		{
			YarnProtos.SerializedExceptionProtoOrBuilder p = viaProto ? proto : builder;
			return p.GetTrace();
		}

		public override SerializedException GetCause()
		{
			YarnProtos.SerializedExceptionProtoOrBuilder p = viaProto ? proto : builder;
			if (p.HasCause())
			{
				return new Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB.SerializedExceptionPBImpl(p
					.GetCause());
			}
			else
			{
				return null;
			}
		}

		public virtual YarnProtos.SerializedExceptionProto GetProto()
		{
			proto = viaProto ? proto : ((YarnProtos.SerializedExceptionProto)builder.Build());
			viaProto = true;
			return proto;
		}

		public override int GetHashCode()
		{
			return GetProto().GetHashCode();
		}

		public override bool Equals(object other)
		{
			if (other == null)
			{
				return false;
			}
			if (other.GetType().IsAssignableFrom(this.GetType()))
			{
				return this.GetProto().Equals(this.GetType().Cast(other).GetProto());
			}
			return false;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.SerializedExceptionProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		private static T InstantiateException<T>(Type cls, string message, Exception cause
			)
			where T : Exception
		{
			Constructor<T> cn;
			T ex = null;
			try
			{
				cn = cls.GetConstructor(typeof(string));
				ex = cn.NewInstance(message);
				Sharpen.Extensions.InitCause(ex, cause);
			}
			catch (SecurityException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (MissingMethodException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (ArgumentException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (InstantiationException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (MemberAccessException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (TargetInvocationException e)
			{
				throw new YarnRuntimeException(e);
			}
			return ex;
		}
	}
}
