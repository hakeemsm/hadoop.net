using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset
{
	public class TestAvailableSpaceVolumeChoosingPolicy
	{
		private const int RandomizedIterations = 10000;

		private const float RandomizedErrorPercent = 0.05f;

		private const long RandomizedAllowedError = (long)(RandomizedErrorPercent * RandomizedIterations
			);

		private static void InitPolicy(VolumeChoosingPolicy<FsVolumeSpi> policy, float preferencePercent
			)
		{
			Configuration conf = new Configuration();
			// Set the threshold to consider volumes imbalanced to 1MB
			conf.SetLong(DFSConfigKeys.DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpaceThresholdKey
				, 1024 * 1024);
			// 1MB
			conf.SetFloat(DFSConfigKeys.DfsDatanodeAvailableSpaceVolumeChoosingPolicyBalancedSpacePreferenceFractionKey
				, preferencePercent);
			((Configurable)policy).SetConf(conf);
		}

		// Test the Round-Robin block-volume fallback path when all volumes are within
		// the threshold.
		/// <exception cref="System.Exception"/>
		public virtual void TestRR()
		{
			AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy = ReflectionUtils.NewInstance
				<AvailableSpaceVolumeChoosingPolicy>(null);
			InitPolicy(policy, 1.0f);
			TestRoundRobinVolumeChoosingPolicy.TestRR(policy);
		}

		// ChooseVolume should throw DiskOutOfSpaceException
		// with volume and block sizes in exception message.
		/// <exception cref="System.Exception"/>
		public virtual void TestRRPolicyExceptionMessage()
		{
			AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy = new AvailableSpaceVolumeChoosingPolicy
				<FsVolumeSpi>();
			InitPolicy(policy, 1.0f);
			TestRoundRobinVolumeChoosingPolicy.TestRRPolicyExceptionMessage(policy);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTwoUnbalancedVolumes()
		{
			AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy = ReflectionUtils.NewInstance
				<AvailableSpaceVolumeChoosingPolicy>(null);
			InitPolicy(policy, 1.0f);
			IList<FsVolumeSpi> volumes = new AList<FsVolumeSpi>();
			// First volume with 1MB free space
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[0].GetAvailable()).ThenReturn(1024L * 1024L);
			// Second volume with 3MB free space, which is a difference of 2MB, more
			// than the threshold of 1MB.
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[1].GetAvailable()).ThenReturn(1024L * 1024L * 3);
			NUnit.Framework.Assert.AreEqual(volumes[1], policy.ChooseVolume(volumes, 100));
			NUnit.Framework.Assert.AreEqual(volumes[1], policy.ChooseVolume(volumes, 100));
			NUnit.Framework.Assert.AreEqual(volumes[1], policy.ChooseVolume(volumes, 100));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestThreeUnbalancedVolumes()
		{
			AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy = ReflectionUtils.NewInstance
				<AvailableSpaceVolumeChoosingPolicy>(null);
			IList<FsVolumeSpi> volumes = new AList<FsVolumeSpi>();
			// First volume with 1MB free space
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[0].GetAvailable()).ThenReturn(1024L * 1024L);
			// Second volume with 3MB free space, which is a difference of 2MB, more
			// than the threshold of 1MB.
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[1].GetAvailable()).ThenReturn(1024L * 1024L * 3);
			// Third volume, again with 3MB free space.
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[2].GetAvailable()).ThenReturn(1024L * 1024L * 3);
			// We should alternate assigning between the two volumes with a lot of free
			// space.
			InitPolicy(policy, 1.0f);
			NUnit.Framework.Assert.AreEqual(volumes[1], policy.ChooseVolume(volumes, 100));
			NUnit.Framework.Assert.AreEqual(volumes[2], policy.ChooseVolume(volumes, 100));
			NUnit.Framework.Assert.AreEqual(volumes[1], policy.ChooseVolume(volumes, 100));
			NUnit.Framework.Assert.AreEqual(volumes[2], policy.ChooseVolume(volumes, 100));
			// All writes should be assigned to the volume with the least free space.
			InitPolicy(policy, 0.0f);
			NUnit.Framework.Assert.AreEqual(volumes[0], policy.ChooseVolume(volumes, 100));
			NUnit.Framework.Assert.AreEqual(volumes[0], policy.ChooseVolume(volumes, 100));
			NUnit.Framework.Assert.AreEqual(volumes[0], policy.ChooseVolume(volumes, 100));
			NUnit.Framework.Assert.AreEqual(volumes[0], policy.ChooseVolume(volumes, 100));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFourUnbalancedVolumes()
		{
			AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy = ReflectionUtils.NewInstance
				<AvailableSpaceVolumeChoosingPolicy>(null);
			IList<FsVolumeSpi> volumes = new AList<FsVolumeSpi>();
			// First volume with 1MB free space
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[0].GetAvailable()).ThenReturn(1024L * 1024L);
			// Second volume with 1MB + 1 byte free space
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[1].GetAvailable()).ThenReturn(1024L * 1024L + 1);
			// Third volume with 3MB free space, which is a difference of 2MB, more
			// than the threshold of 1MB.
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[2].GetAvailable()).ThenReturn(1024L * 1024L * 3);
			// Fourth volume, again with 3MB free space.
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[3].GetAvailable()).ThenReturn(1024L * 1024L * 3);
			// We should alternate assigning between the two volumes with a lot of free
			// space.
			InitPolicy(policy, 1.0f);
			NUnit.Framework.Assert.AreEqual(volumes[2], policy.ChooseVolume(volumes, 100));
			NUnit.Framework.Assert.AreEqual(volumes[3], policy.ChooseVolume(volumes, 100));
			NUnit.Framework.Assert.AreEqual(volumes[2], policy.ChooseVolume(volumes, 100));
			NUnit.Framework.Assert.AreEqual(volumes[3], policy.ChooseVolume(volumes, 100));
			// We should alternate assigning between the two volumes with less free
			// space.
			InitPolicy(policy, 0.0f);
			NUnit.Framework.Assert.AreEqual(volumes[0], policy.ChooseVolume(volumes, 100));
			NUnit.Framework.Assert.AreEqual(volumes[1], policy.ChooseVolume(volumes, 100));
			NUnit.Framework.Assert.AreEqual(volumes[0], policy.ChooseVolume(volumes, 100));
			NUnit.Framework.Assert.AreEqual(volumes[1], policy.ChooseVolume(volumes, 100));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNotEnoughSpaceOnSelectedVolume()
		{
			AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy = ReflectionUtils.NewInstance
				<AvailableSpaceVolumeChoosingPolicy>(null);
			IList<FsVolumeSpi> volumes = new AList<FsVolumeSpi>();
			// First volume with 1MB free space
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[0].GetAvailable()).ThenReturn(1024L * 1024L);
			// Second volume with 3MB free space, which is a difference of 2MB, more
			// than the threshold of 1MB.
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[1].GetAvailable()).ThenReturn(1024L * 1024L * 3);
			// All writes should be assigned to the volume with the least free space.
			// However, if the volume with the least free space doesn't have enough
			// space to accept the replica size, and another volume does have enough
			// free space, that should be chosen instead.
			InitPolicy(policy, 0.0f);
			NUnit.Framework.Assert.AreEqual(volumes[1], policy.ChooseVolume(volumes, 1024L * 
				1024L * 2));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAvailableSpaceChanges()
		{
			AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy = ReflectionUtils.NewInstance
				<AvailableSpaceVolumeChoosingPolicy>(null);
			InitPolicy(policy, 1.0f);
			IList<FsVolumeSpi> volumes = new AList<FsVolumeSpi>();
			// First volume with 1MB free space
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[0].GetAvailable()).ThenReturn(1024L * 1024L);
			// Second volume with 3MB free space, which is a difference of 2MB, more
			// than the threshold of 1MB.
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[1].GetAvailable()).ThenReturn(1024L * 1024L * 3)
				.ThenReturn(1024L * 1024L * 3).ThenReturn(1024L * 1024L * 3).ThenReturn(1024L * 
				1024L * 1);
			// After the third check, return 1MB.
			// Should still be able to get a volume for the replica even though the
			// available space on the second volume changed.
			NUnit.Framework.Assert.AreEqual(volumes[1], policy.ChooseVolume(volumes, 100));
		}

		/// <exception cref="System.Exception"/>
		public virtual void RandomizedTest1()
		{
			DoRandomizedTest(0.75f, 1, 1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void RandomizedTest2()
		{
			DoRandomizedTest(0.75f, 5, 1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void RandomizedTest3()
		{
			DoRandomizedTest(0.75f, 1, 5);
		}

		/// <exception cref="System.Exception"/>
		public virtual void RandomizedTest4()
		{
			DoRandomizedTest(0.90f, 5, 1);
		}

		/*
		* Ensure that we randomly select the lesser-used volumes with appropriate
		* frequency.
		*/
		/// <exception cref="System.Exception"/>
		public virtual void DoRandomizedTest(float preferencePercent, int lowSpaceVolumes
			, int highSpaceVolumes)
		{
			Random random = new Random(123L);
			AvailableSpaceVolumeChoosingPolicy<FsVolumeSpi> policy = new AvailableSpaceVolumeChoosingPolicy
				<FsVolumeSpi>(random);
			IList<FsVolumeSpi> volumes = new AList<FsVolumeSpi>();
			// Volumes with 1MB free space
			for (int i = 0; i < lowSpaceVolumes; i++)
			{
				FsVolumeSpi volume = Org.Mockito.Mockito.Mock<FsVolumeSpi>();
				Org.Mockito.Mockito.When(volume.GetAvailable()).ThenReturn(1024L * 1024L);
				volumes.AddItem(volume);
			}
			// Volumes with 3MB free space
			for (int i_1 = 0; i_1 < highSpaceVolumes; i_1++)
			{
				FsVolumeSpi volume = Org.Mockito.Mockito.Mock<FsVolumeSpi>();
				Org.Mockito.Mockito.When(volume.GetAvailable()).ThenReturn(1024L * 1024L * 3);
				volumes.AddItem(volume);
			}
			InitPolicy(policy, preferencePercent);
			long lowAvailableSpaceVolumeSelected = 0;
			long highAvailableSpaceVolumeSelected = 0;
			for (int i_2 = 0; i_2 < RandomizedIterations; i_2++)
			{
				FsVolumeSpi volume = policy.ChooseVolume(volumes, 100);
				for (int j = 0; j < volumes.Count; j++)
				{
					// Note how many times the first low available volume was selected
					if (volume == volumes[j] && j == 0)
					{
						lowAvailableSpaceVolumeSelected++;
					}
					// Note how many times the first high available volume was selected
					if (volume == volumes[j] && j == lowSpaceVolumes)
					{
						highAvailableSpaceVolumeSelected++;
						break;
					}
				}
			}
			// Calculate the expected ratio of how often low available space volumes
			// were selected vs. high available space volumes.
			float expectedSelectionRatio = preferencePercent / (1 - preferencePercent);
			GenericTestUtils.AssertValueNear((long)(lowAvailableSpaceVolumeSelected * expectedSelectionRatio
				), highAvailableSpaceVolumeSelected, RandomizedAllowedError);
		}
	}
}
