using Sharpen;

namespace org.apache.hadoop.ipc
{
	[System.Serializable]
	public class RemoteException : System.IO.IOException
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

		public RemoteException(string className, string msg, org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
			 erCode)
			: base(msg)
		{
			this.className = className;
			if (erCode != null)
			{
				errorCode = erCode.getNumber();
			}
			else
			{
				errorCode = -1;
			}
		}

		public virtual string getClassName()
		{
			return className;
		}

		public virtual org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
			 getErrorCode()
		{
			return org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
				.valueOf(errorCode);
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
		public virtual System.IO.IOException unwrapRemoteException(params java.lang.Class
			[] lookupTypes)
		{
			if (lookupTypes == null)
			{
				return this;
			}
			foreach (java.lang.Class lookupClass in lookupTypes)
			{
				if (!lookupClass.getName().Equals(getClassName()))
				{
					continue;
				}
				try
				{
					return instantiateException(lookupClass.asSubclass<System.IO.IOException>());
				}
				catch (System.Exception)
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
		public virtual System.IO.IOException unwrapRemoteException()
		{
			try
			{
				java.lang.Class realClass = java.lang.Class.forName(getClassName());
				return instantiateException(realClass.asSubclass<System.IO.IOException>());
			}
			catch (System.Exception)
			{
			}
			// cannot instantiate the original exception, just return this
			return this;
		}

		/// <exception cref="System.Exception"/>
		private System.IO.IOException instantiateException(java.lang.Class cls)
		{
			java.lang.reflect.Constructor<System.IO.IOException> cn = cls.getConstructor(Sharpen.Runtime.getClassForType
				(typeof(string)));
			cn.setAccessible(true);
			System.IO.IOException ex = cn.newInstance(this.Message);
			ex.initCause(this);
			return ex;
		}

		/// <summary>Create RemoteException from attributes</summary>
		public static org.apache.hadoop.ipc.RemoteException valueOf(org.xml.sax.Attributes
			 attrs)
		{
			return new org.apache.hadoop.ipc.RemoteException(attrs.getValue("class"), attrs.getValue
				("message"));
		}

		public override string ToString()
		{
			return Sharpen.Runtime.getClassForObject(this).getName() + "(" + className + "): "
				 + Message;
		}
	}
}
