using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store
{
	/// <summary>All test classes that test an SCMStore implementation must extend this class.
	/// 	</summary>
	public abstract class SCMStoreBaseTest
	{
		/// <summary>Get the SCMStore implementation class associated with this test class.</summary>
		internal abstract Type GetStoreClass();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestZeroArgConstructor()
		{
			// Test that the SCMStore implementation class is compatible with
			// ReflectionUtils#newInstance
			ReflectionUtils.NewInstance(GetStoreClass(), new Configuration());
		}
	}
}
