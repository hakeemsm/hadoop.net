using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Policies;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class TestSchedulingPolicy
	{
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		public virtual void TestParseSchedulingPolicy()
		{
			// Class name
			SchedulingPolicy sm = SchedulingPolicy.Parse(typeof(FairSharePolicy).FullName);
			NUnit.Framework.Assert.IsTrue("Invalid scheduler name", sm.GetName().Equals(FairSharePolicy
				.Name));
			// Canonical name
			sm = SchedulingPolicy.Parse(typeof(FairSharePolicy).GetCanonicalName());
			NUnit.Framework.Assert.IsTrue("Invalid scheduler name", sm.GetName().Equals(FairSharePolicy
				.Name));
			// Class
			sm = SchedulingPolicy.GetInstance(typeof(FairSharePolicy));
			NUnit.Framework.Assert.IsTrue("Invalid scheduler name", sm.GetName().Equals(FairSharePolicy
				.Name));
			// Shortname - drf
			sm = SchedulingPolicy.Parse("drf");
			NUnit.Framework.Assert.IsTrue("Invalid scheduler name", sm.GetName().Equals(DominantResourceFairnessPolicy
				.Name));
			// Shortname - fair
			sm = SchedulingPolicy.Parse("fair");
			NUnit.Framework.Assert.IsTrue("Invalid scheduler name", sm.GetName().Equals(FairSharePolicy
				.Name));
			// Shortname - fifo
			sm = SchedulingPolicy.Parse("fifo");
			NUnit.Framework.Assert.IsTrue("Invalid scheduler name", sm.GetName().Equals(FifoPolicy
				.Name));
		}

		/// <summary>
		/// Trivial tests that make sure
		/// <see cref="SchedulingPolicy.IsApplicableTo(SchedulingPolicy, byte)"/>
		/// works as
		/// expected for the possible values of depth
		/// </summary>
		/// <exception cref="AllocationConfigurationException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		public virtual void TestIsApplicableTo()
		{
			string Err = "Broken SchedulingPolicy#isApplicableTo";
			// fifo
			SchedulingPolicy policy = SchedulingPolicy.Parse("fifo");
			NUnit.Framework.Assert.IsTrue(Err, SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy
				.DepthLeaf));
			NUnit.Framework.Assert.IsFalse(Err, SchedulingPolicy.IsApplicableTo(SchedulingPolicy
				.Parse("fifo"), SchedulingPolicy.DepthIntermediate));
			NUnit.Framework.Assert.IsFalse(Err, SchedulingPolicy.IsApplicableTo(SchedulingPolicy
				.Parse("fifo"), SchedulingPolicy.DepthRoot));
			// fair
			policy = SchedulingPolicy.Parse("fair");
			NUnit.Framework.Assert.IsTrue(Err, SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy
				.DepthLeaf));
			NUnit.Framework.Assert.IsTrue(Err, SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy
				.DepthIntermediate));
			NUnit.Framework.Assert.IsTrue(Err, SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy
				.DepthRoot));
			NUnit.Framework.Assert.IsTrue(Err, SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy
				.DepthParent));
			NUnit.Framework.Assert.IsTrue(Err, SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy
				.DepthAny));
			// drf
			policy = SchedulingPolicy.Parse("drf");
			NUnit.Framework.Assert.IsTrue(Err, SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy
				.DepthLeaf));
			NUnit.Framework.Assert.IsTrue(Err, SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy
				.DepthIntermediate));
			NUnit.Framework.Assert.IsTrue(Err, SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy
				.DepthRoot));
			NUnit.Framework.Assert.IsTrue(Err, SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy
				.DepthParent));
			NUnit.Framework.Assert.IsTrue(Err, SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy
				.DepthAny));
			policy = Org.Mockito.Mockito.Mock<SchedulingPolicy>();
			Org.Mockito.Mockito.When(policy.GetApplicableDepth()).ThenReturn(SchedulingPolicy
				.DepthParent);
			NUnit.Framework.Assert.IsTrue(Err, SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy
				.DepthIntermediate));
			NUnit.Framework.Assert.IsTrue(Err, SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy
				.DepthRoot));
			NUnit.Framework.Assert.IsTrue(Err, SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy
				.DepthParent));
			NUnit.Framework.Assert.IsFalse(Err, SchedulingPolicy.IsApplicableTo(policy, SchedulingPolicy
				.DepthAny));
		}
	}
}
