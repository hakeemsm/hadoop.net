using System;
using System.IO;
using Org.Apache.Hadoop.Ipc.Protobuf;
using Org.Xml.Sax;

using Reflect;

namespace Org.Apache.Hadoop.Ipc
{
	[System.Serializable]
	public class RemoteException : IOException
	{
		/// <summary>For java.io.Serializable</summary>
		private const long serialVersionUID = 1L;

		private readonly int errorCode;

		private string className;

		public RemoteException(string className, string msg)
			: base(msg)
		{
			this.className = className;
			errorCode = -1;
		}

		public RemoteException(string className, string msg, RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
			 erCode)
			: base(msg)
		{
			this.className = className;
			if (erCode != null)
			{
				errorCode = erCode.GetNumber();
			}
			else
			{
				errorCode = -1;
			}
		}

		public virtual string GetClassName()
		{
			return className;
		}

		public virtual RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto GetErrorCode
			()
		{
			return RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ValueOf(errorCode
				);
		}

		/// <summary>
		/// If this remote exception wraps up one of the lookupTypes
		/// then return this exception.
		/// </summary>
		/// <remarks>
		/// If this remote exception wraps up one of the lookupTypes
		/// then return this exception.
		/// <p>
		/// Unwraps any IOException.
		/// </remarks>
		/// <param name="lookupTypes">the desired exception class.</param>
		/// <returns>IOException, which is either the lookupClass exception or this.</returns>
		public virtual IOException UnwrapRemoteException(params Type[] lookupTypes)
		{
			if (lookupTypes == null)
			{
				return this;
			}
			foreach (Type lookupClass in lookupTypes)
			{
				if (!lookupClass.FullName.Equals(GetClassName()))
				{
					continue;
				}
				try
				{
					return InstantiateException(lookupClass.AsSubclass<IOException>());
				}
				catch (Exception)
				{
					// cannot instantiate lookupClass, just return this
					return this;
				}
			}
			// wrapped up exception is not in lookupTypes, just return this
			return this;
		}

		/// <summary>Instantiate and return the exception wrapped up by this remote exception.
		/// 	</summary>
		/// <remarks>
		/// Instantiate and return the exception wrapped up by this remote exception.
		/// <p> This unwraps any <code>Throwable</code> that has a constructor taking
		/// a <code>String</code> as a parameter.
		/// Otherwise it returns this.
		/// </remarks>
		/// <returns><code>Throwable</returns>
		public virtual IOException UnwrapRemoteException()
		{
			try
			{
				Type realClass = Runtime.GetType(GetClassName());
				return InstantiateException(realClass.AsSubclass<IOException>());
			}
			catch (Exception)
			{
			}
			// cannot instantiate the original exception, just return this
			return this;
		}

		/// <exception cref="System.Exception"/>
		private IOException InstantiateException(Type cls)
		{
			Constructor<IOException> cn = cls.GetConstructor(typeof(string));
			IOException ex = cn.NewInstance(this.Message);
			Extensions.InitCause(ex, this);
			return ex;
		}

		/// <summary>Create RemoteException from attributes</summary>
		public static Org.Apache.Hadoop.Ipc.RemoteException ValueOf(Attributes attrs)
		{
			return new Org.Apache.Hadoop.Ipc.RemoteException(attrs.GetValue("class"), attrs.GetValue
				("message"));
		}

		public override string ToString()
		{
			return GetType().FullName + "(" + className + "): " + Message;
		}
	}
}
