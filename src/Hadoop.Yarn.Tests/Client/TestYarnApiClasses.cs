using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class TestYarnApiClasses
	{
		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		/// <summary>Simple test Resource request.</summary>
		/// <remarks>
		/// Simple test Resource request.
		/// Test hashCode, equals and compare.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestResourceRequest()
		{
			Resource resource = recordFactory.NewRecordInstance<Resource>();
			Priority priority = recordFactory.NewRecordInstance<Priority>();
			ResourceRequest original = ResourceRequest.NewInstance(priority, "localhost", resource
				, 2);
			ResourceRequest copy = ResourceRequest.NewInstance(priority, "localhost", resource
				, 2);
			NUnit.Framework.Assert.IsTrue(original.Equals(copy));
			NUnit.Framework.Assert.AreEqual(0, original.CompareTo(copy));
			NUnit.Framework.Assert.IsTrue(original.GetHashCode() == copy.GetHashCode());
			copy.SetNumContainers(1);
			NUnit.Framework.Assert.IsFalse(original.Equals(copy));
			NUnit.Framework.Assert.AreNotSame(0, original.CompareTo(copy));
			NUnit.Framework.Assert.IsFalse(original.GetHashCode() == copy.GetHashCode());
		}

		/// <summary>Test CancelDelegationTokenRequestPBImpl.</summary>
		/// <remarks>
		/// Test CancelDelegationTokenRequestPBImpl.
		/// Test a transformation to prototype and back
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestCancelDelegationTokenRequestPBImpl()
		{
			Token token = GetDelegationToken();
			CancelDelegationTokenRequestPBImpl original = new CancelDelegationTokenRequestPBImpl
				();
			original.SetDelegationToken(token);
			SecurityProtos.CancelDelegationTokenRequestProto protoType = original.GetProto();
			CancelDelegationTokenRequestPBImpl copy = new CancelDelegationTokenRequestPBImpl(
				protoType);
			NUnit.Framework.Assert.IsNotNull(copy.GetDelegationToken());
			//compare source and converted
			NUnit.Framework.Assert.AreEqual(token, copy.GetDelegationToken());
		}

		/// <summary>Test RenewDelegationTokenRequestPBImpl.</summary>
		/// <remarks>
		/// Test RenewDelegationTokenRequestPBImpl.
		/// Test a transformation to prototype and back
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestRenewDelegationTokenRequestPBImpl()
		{
			Token token = GetDelegationToken();
			RenewDelegationTokenRequestPBImpl original = new RenewDelegationTokenRequestPBImpl
				();
			original.SetDelegationToken(token);
			SecurityProtos.RenewDelegationTokenRequestProto protoType = original.GetProto();
			RenewDelegationTokenRequestPBImpl copy = new RenewDelegationTokenRequestPBImpl(protoType
				);
			NUnit.Framework.Assert.IsNotNull(copy.GetDelegationToken());
			//compare source and converted
			NUnit.Framework.Assert.AreEqual(token, copy.GetDelegationToken());
		}

		private Token GetDelegationToken()
		{
			return Token.NewInstance(new byte[0], string.Empty, new byte[0], string.Empty);
		}
	}
}
