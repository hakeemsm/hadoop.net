using System.Reflection;
using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO.Retry;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// Tests to make sure all the protocol class public methods have
	/// either
	/// <see cref="Org.Apache.Hadoop.IO.Retry.Idempotent"/>
	/// or
	/// <see cref="Org.Apache.Hadoop.IO.Retry.AtMostOnce"/>
	/// once annotations.
	/// </summary>
	public class TestAnnotations
	{
		[NUnit.Framework.Test]
		public virtual void CheckAnnotations()
		{
			MethodInfo[] methods = typeof(NamenodeProtocols).GetMethods();
			foreach (MethodInfo m in methods)
			{
				NUnit.Framework.Assert.IsTrue("Idempotent or AtMostOnce annotation is not present "
					 + m, m.IsAnnotationPresent(typeof(Idempotent)) || m.IsAnnotationPresent(typeof(
					AtMostOnce)));
			}
		}
	}
}
