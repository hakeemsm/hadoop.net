using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO.Nativeio;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Shortcircuit
{
	public class TestShortCircuitShm
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestShortCircuitShm));

		private static readonly FilePath TestBase = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp"));

		[SetUp]
		public virtual void Before()
		{
			Assume.AssumeTrue(null == SharedFileDescriptorFactory.GetLoadingFailureReason());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStartupShutdown()
		{
			FilePath path = new FilePath(TestBase, "testStartupShutdown");
			path.Mkdirs();
			SharedFileDescriptorFactory factory = SharedFileDescriptorFactory.Create("shm_", 
				new string[] { path.GetAbsolutePath() });
			FileInputStream stream = factory.CreateDescriptor("testStartupShutdown", 4096);
			ShortCircuitShm shm = new ShortCircuitShm(ShortCircuitShm.ShmId.CreateRandom(), stream
				);
			shm.Free();
			stream.Close();
			FileUtil.FullyDelete(path);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAllocateSlots()
		{
			FilePath path = new FilePath(TestBase, "testAllocateSlots");
			path.Mkdirs();
			SharedFileDescriptorFactory factory = SharedFileDescriptorFactory.Create("shm_", 
				new string[] { path.GetAbsolutePath() });
			FileInputStream stream = factory.CreateDescriptor("testAllocateSlots", 4096);
			ShortCircuitShm shm = new ShortCircuitShm(ShortCircuitShm.ShmId.CreateRandom(), stream
				);
			int numSlots = 0;
			AList<ShortCircuitShm.Slot> slots = new AList<ShortCircuitShm.Slot>();
			while (!shm.IsFull())
			{
				ShortCircuitShm.Slot slot = shm.AllocAndRegisterSlot(new ExtendedBlockId(123L, "test_bp1"
					));
				slots.AddItem(slot);
				numSlots++;
			}
			Log.Info("allocated " + numSlots + " slots before running out.");
			int slotIdx = 0;
			for (IEnumerator<ShortCircuitShm.Slot> iter = shm.SlotIterator(); iter.HasNext(); )
			{
				NUnit.Framework.Assert.IsTrue(slots.Contains(iter.Next()));
			}
			foreach (ShortCircuitShm.Slot slot_1 in slots)
			{
				NUnit.Framework.Assert.IsFalse(slot_1.AddAnchor());
				NUnit.Framework.Assert.AreEqual(slotIdx++, slot_1.GetSlotIdx());
			}
			foreach (ShortCircuitShm.Slot slot_2 in slots)
			{
				slot_2.MakeAnchorable();
			}
			foreach (ShortCircuitShm.Slot slot_3 in slots)
			{
				NUnit.Framework.Assert.IsTrue(slot_3.AddAnchor());
			}
			foreach (ShortCircuitShm.Slot slot_4 in slots)
			{
				slot_4.RemoveAnchor();
			}
			foreach (ShortCircuitShm.Slot slot_5 in slots)
			{
				shm.UnregisterSlot(slot_5.GetSlotIdx());
				slot_5.MakeInvalid();
			}
			shm.Free();
			stream.Close();
			FileUtil.FullyDelete(path);
		}
	}
}
