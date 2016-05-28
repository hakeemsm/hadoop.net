using System;
using System.IO;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestLeaseRenewer
	{
		private readonly string FakeAuthority = "hdfs://nn1/";

		private readonly UserGroupInformation FakeUgiA = UserGroupInformation.CreateUserForTesting
			("myuser", new string[] { "group1" });

		private readonly UserGroupInformation FakeUgiB = UserGroupInformation.CreateUserForTesting
			("myuser", new string[] { "group1" });

		private DFSClient MockDfsclient;

		private LeaseRenewer renewer;

		/// <summary>Cause renewals often so test runs quickly.</summary>
		private const long FastGracePeriod = 100L;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetupMocksAndRenewer()
		{
			MockDfsclient = CreateMockClient();
			renewer = LeaseRenewer.GetInstance(FakeAuthority, FakeUgiA, MockDfsclient);
			renewer.SetGraceSleepPeriod(FastGracePeriod);
		}

		private DFSClient CreateMockClient()
		{
			DFSClient mock = Org.Mockito.Mockito.Mock<DFSClient>();
			Org.Mockito.Mockito.DoReturn(true).When(mock).IsClientRunning();
			Org.Mockito.Mockito.DoReturn((int)FastGracePeriod).When(mock).GetHdfsTimeout();
			Org.Mockito.Mockito.DoReturn("myclient").When(mock).GetClientName();
			return mock;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInstanceSharing()
		{
			// Two lease renewers with the same UGI should return
			// the same instance
			LeaseRenewer lr = LeaseRenewer.GetInstance(FakeAuthority, FakeUgiA, MockDfsclient
				);
			LeaseRenewer lr2 = LeaseRenewer.GetInstance(FakeAuthority, FakeUgiA, MockDfsclient
				);
			NUnit.Framework.Assert.AreSame(lr, lr2);
			// But a different UGI should return a different instance
			LeaseRenewer lr3 = LeaseRenewer.GetInstance(FakeAuthority, FakeUgiB, MockDfsclient
				);
			NUnit.Framework.Assert.AreNotSame(lr, lr3);
			// A different authority with same UGI should also be a different
			// instance.
			LeaseRenewer lr4 = LeaseRenewer.GetInstance("someOtherAuthority", FakeUgiB, MockDfsclient
				);
			NUnit.Framework.Assert.AreNotSame(lr, lr4);
			NUnit.Framework.Assert.AreNotSame(lr3, lr4);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenewal()
		{
			// Keep track of how many times the lease gets renewed
			AtomicInteger leaseRenewalCount = new AtomicInteger();
			Org.Mockito.Mockito.DoAnswer(new _Answer_99(leaseRenewalCount)).When(MockDfsclient
				).RenewLease();
			// Set up a file so that we start renewing our lease.
			DFSOutputStream mockStream = Org.Mockito.Mockito.Mock<DFSOutputStream>();
			long fileId = 123L;
			renewer.Put(fileId, mockStream, MockDfsclient);
			// Wait for lease to get renewed
			long failTime = Time.MonotonicNow() + 5000;
			while (Time.MonotonicNow() < failTime && leaseRenewalCount.Get() == 0)
			{
				Sharpen.Thread.Sleep(50);
			}
			if (leaseRenewalCount.Get() == 0)
			{
				NUnit.Framework.Assert.Fail("Did not renew lease at all!");
			}
			renewer.CloseFile(fileId, MockDfsclient);
		}

		private sealed class _Answer_99 : Answer<bool>
		{
			public _Answer_99(AtomicInteger leaseRenewalCount)
			{
				this.leaseRenewalCount = leaseRenewalCount;
			}

			/// <exception cref="System.Exception"/>
			public bool Answer(InvocationOnMock invocation)
			{
				leaseRenewalCount.IncrementAndGet();
				return true;
			}

			private readonly AtomicInteger leaseRenewalCount;
		}

		/// <summary>Regression test for HDFS-2810.</summary>
		/// <remarks>
		/// Regression test for HDFS-2810. In this bug, the LeaseRenewer has handles
		/// to several DFSClients with the same name, the first of which has no files
		/// open. Previously, this was causing the lease to not get renewed.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestManyDfsClientsWhereSomeNotOpen()
		{
			// First DFSClient has no files open so doesn't renew leases.
			DFSClient mockClient1 = CreateMockClient();
			Org.Mockito.Mockito.DoReturn(false).When(mockClient1).RenewLease();
			NUnit.Framework.Assert.AreSame(renewer, LeaseRenewer.GetInstance(FakeAuthority, FakeUgiA
				, mockClient1));
			// Set up a file so that we start renewing our lease.
			DFSOutputStream mockStream1 = Org.Mockito.Mockito.Mock<DFSOutputStream>();
			long fileId = 456L;
			renewer.Put(fileId, mockStream1, mockClient1);
			// Second DFSClient does renew lease
			DFSClient mockClient2 = CreateMockClient();
			Org.Mockito.Mockito.DoReturn(true).When(mockClient2).RenewLease();
			NUnit.Framework.Assert.AreSame(renewer, LeaseRenewer.GetInstance(FakeAuthority, FakeUgiA
				, mockClient2));
			// Set up a file so that we start renewing our lease.
			DFSOutputStream mockStream2 = Org.Mockito.Mockito.Mock<DFSOutputStream>();
			renewer.Put(fileId, mockStream2, mockClient2);
			// Wait for lease to get renewed
			GenericTestUtils.WaitFor(new _Supplier_156(mockClient1, mockClient2), 100, 10000);
			// should not throw!
			renewer.CloseFile(fileId, mockClient1);
			renewer.CloseFile(fileId, mockClient2);
		}

		private sealed class _Supplier_156 : Supplier<bool>
		{
			public _Supplier_156(DFSClient mockClient1, DFSClient mockClient2)
			{
				this.mockClient1 = mockClient1;
				this.mockClient2 = mockClient2;
			}

			public bool Get()
			{
				try
				{
					Org.Mockito.Mockito.Verify(mockClient1, Org.Mockito.Mockito.AtLeastOnce()).RenewLease
						();
					Org.Mockito.Mockito.Verify(mockClient2, Org.Mockito.Mockito.AtLeastOnce()).RenewLease
						();
					return true;
				}
				catch (Exception err)
				{
					LeaseRenewer.Log.Warn("Not yet satisfied", err);
					return false;
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
			}

			private readonly DFSClient mockClient1;

			private readonly DFSClient mockClient2;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestThreadName()
		{
			DFSOutputStream mockStream = Org.Mockito.Mockito.Mock<DFSOutputStream>();
			long fileId = 789L;
			NUnit.Framework.Assert.IsFalse("Renewer not initially running", renewer.IsRunning
				());
			// Pretend to open a file
			renewer.Put(fileId, mockStream, MockDfsclient);
			NUnit.Framework.Assert.IsTrue("Renewer should have started running", renewer.IsRunning
				());
			// Check the thread name is reasonable
			string threadName = renewer.GetDaemonName();
			NUnit.Framework.Assert.AreEqual("LeaseRenewer:myuser@hdfs://nn1/", threadName);
			// Pretend to close the file
			renewer.CloseFile(fileId, MockDfsclient);
			renewer.SetEmptyTime(Time.MonotonicNow());
			// Should stop the renewer running within a few seconds
			long failTime = Time.MonotonicNow() + 5000;
			while (renewer.IsRunning() && Time.MonotonicNow() < failTime)
			{
				Sharpen.Thread.Sleep(50);
			}
			NUnit.Framework.Assert.IsFalse(renewer.IsRunning());
		}
	}
}
