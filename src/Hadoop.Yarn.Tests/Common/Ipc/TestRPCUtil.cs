using System;
using System.IO;
using Com.Google.Protobuf;
using NUnit.Framework;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Ipc
{
	public class TestRPCUtil
	{
		[NUnit.Framework.Test]
		public virtual void TestUnknownExceptionUnwrapping()
		{
			Type exception = typeof(YarnException);
			string className = "UnknownException.class";
			VerifyRemoteExceptionUnwrapping(exception, className);
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoteIOExceptionUnwrapping()
		{
			Type exception = typeof(IOException);
			VerifyRemoteExceptionUnwrapping(exception, exception.FullName);
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoteIOExceptionDerivativeUnwrapping()
		{
			// Test IOException sub-class
			Type exception = typeof(FileNotFoundException);
			VerifyRemoteExceptionUnwrapping(exception, exception.FullName);
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoteYarnExceptionUnwrapping()
		{
			Type exception = typeof(YarnException);
			VerifyRemoteExceptionUnwrapping(exception, exception.FullName);
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoteYarnExceptionDerivativeUnwrapping()
		{
			Type exception = typeof(TestRPCUtil.YarnTestException);
			VerifyRemoteExceptionUnwrapping(exception, exception.FullName);
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoteRuntimeExceptionUnwrapping()
		{
			Type exception = typeof(ArgumentNullException);
			VerifyRemoteExceptionUnwrapping(exception, exception.FullName);
		}

		[NUnit.Framework.Test]
		public virtual void TestUnexpectedRemoteExceptionUnwrapping()
		{
			// Non IOException, YarnException thrown by the remote side.
			Type exception = typeof(Exception);
			VerifyRemoteExceptionUnwrapping(typeof(RemoteException), exception.FullName);
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoteYarnExceptionWithoutStringConstructor()
		{
			// Derivatives of YarnException should always define a string constructor.
			Type exception = typeof(TestRPCUtil.YarnTestExceptionNoConstructor);
			VerifyRemoteExceptionUnwrapping(typeof(RemoteException), exception.FullName);
		}

		[NUnit.Framework.Test]
		public virtual void TestRPCServiceExceptionUnwrapping()
		{
			string message = "ServiceExceptionMessage";
			ServiceException se = new ServiceException(message);
			Exception t = null;
			try
			{
				RPCUtil.UnwrapAndThrowException(se);
			}
			catch (Exception thrown)
			{
				t = thrown;
			}
			NUnit.Framework.Assert.IsTrue(typeof(IOException).IsInstanceOfType(t));
			NUnit.Framework.Assert.IsTrue(t.Message.Contains(message));
		}

		[NUnit.Framework.Test]
		public virtual void TestRPCIOExceptionUnwrapping()
		{
			string message = "DirectIOExceptionMessage";
			IOException ioException = new FileNotFoundException(message);
			ServiceException se = new ServiceException(ioException);
			Exception t = null;
			try
			{
				RPCUtil.UnwrapAndThrowException(se);
			}
			catch (Exception thrown)
			{
				t = thrown;
			}
			NUnit.Framework.Assert.IsTrue(typeof(FileNotFoundException).IsInstanceOfType(t));
			NUnit.Framework.Assert.IsTrue(t.Message.Contains(message));
		}

		[NUnit.Framework.Test]
		public virtual void TestRPCRuntimeExceptionUnwrapping()
		{
			string message = "RPCRuntimeExceptionUnwrapping";
			RuntimeException re = new ArgumentNullException(message);
			ServiceException se = new ServiceException(re);
			Exception t = null;
			try
			{
				RPCUtil.UnwrapAndThrowException(se);
			}
			catch (Exception thrown)
			{
				t = thrown;
			}
			NUnit.Framework.Assert.IsTrue(typeof(ArgumentNullException).IsInstanceOfType(t));
			NUnit.Framework.Assert.IsTrue(t.Message.Contains(message));
		}

		private void VerifyRemoteExceptionUnwrapping(Type expectedLocalException, string 
			realExceptionClassName)
		{
			string message = realExceptionClassName + "Message";
			RemoteException re = new RemoteException(realExceptionClassName, message);
			ServiceException se = new ServiceException(re);
			Exception t = null;
			try
			{
				RPCUtil.UnwrapAndThrowException(se);
			}
			catch (Exception thrown)
			{
				t = thrown;
			}
			NUnit.Framework.Assert.IsTrue("Expected exception [" + expectedLocalException + "] but found "
				 + t, expectedLocalException.IsInstanceOfType(t));
			NUnit.Framework.Assert.IsTrue("Expected message [" + message + "] but found " + t
				.Message, t.Message.Contains(message));
		}

		[System.Serializable]
		private class YarnTestException : YarnException
		{
			private const long serialVersionUID = 1L;

			public YarnTestException(string message)
				: base(message)
			{
			}
		}

		[System.Serializable]
		private class YarnTestExceptionNoConstructor : YarnException
		{
			private const long serialVersionUID = 1L;
		}
	}
}
