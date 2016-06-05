using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>
	/// Test
	/// <see cref="Diff{K, E}"/>
	/// with
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.INode"/>
	/// .
	/// </summary>
	public class TestDiff
	{
		private static readonly Random Random = new Random();

		private const int UndoTestP = 10;

		private static readonly PermissionStatus Perm = PermissionStatus.CreateImmutable(
			"user", "group", FsPermission.CreateImmutable((short)0));

		internal static int NextStep(int n)
		{
			return n == 0 ? 1 : 10 * n;
		}

		/// <summary>Test directory diff.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDiff()
		{
			for (int startSize = 0; startSize <= 10000; startSize = NextStep(startSize))
			{
				for (int m = 0; m <= 10000; m = NextStep(m))
				{
					RunDiffTest(startSize, m);
				}
			}
		}

		/// <summary>
		/// The following are the step of the diff test:
		/// 1) Initialize the previous list and add s elements to it,
		/// where s = startSize.
		/// </summary>
		/// <remarks>
		/// The following are the step of the diff test:
		/// 1) Initialize the previous list and add s elements to it,
		/// where s = startSize.
		/// 2) Initialize the current list by coping all elements from the previous list
		/// 3) Initialize an empty diff object.
		/// 4) Make m modifications to the current list, where m = numModifications.
		/// Record the modifications in diff at the same time.
		/// 5) Test if current == previous + diff and previous == current - diff.
		/// 6) Test accessPrevious and accessCurrent.
		/// </remarks>
		/// <param name="startSize"/>
		/// <param name="numModifications"/>
		/// <param name="computeDiff"/>
		internal virtual void RunDiffTest(int startSize, int numModifications)
		{
			int width = FindWidth(startSize + numModifications);
			System.Console.Out.WriteLine("\nstartSize=" + startSize + ", numModifications=" +
				 numModifications + ", width=" + width);
			// initialize previous
			IList<INode> previous = new AList<INode>();
			int n = 0;
			for (; n < startSize; n++)
			{
				previous.AddItem(NewINode(n, width));
			}
			// make modifications to current and record the diff
			IList<INode> current = new AList<INode>(previous);
			IList<Diff<byte[], INode>> diffs = new AList<Diff<byte[], INode>>();
			for (int j = 0; j < 5; j++)
			{
				diffs.AddItem(new Diff<byte[], INode>());
			}
			for (int m = 0; m < numModifications; m++)
			{
				int j_1 = m * diffs.Count / numModifications;
				// if current is empty, the next operation must be create;
				// otherwise, randomly pick an operation.
				int nextOperation = current.IsEmpty() ? 1 : Random.Next(3) + 1;
				switch (nextOperation)
				{
					case 1:
					{
						// create
						INode i = NewINode(n++, width);
						Create(i, current, diffs[j_1]);
						break;
					}

					case 2:
					{
						// delete
						INode i = current[Random.Next(current.Count)];
						Delete(i, current, diffs[j_1]);
						break;
					}

					case 3:
					{
						// modify
						INode i = current[Random.Next(current.Count)];
						Modify(i, current, diffs[j_1]);
						break;
					}
				}
			}
			{
				// check if current == previous + diffs
				IList<INode> c = previous;
				for (int i = 0; i < diffs.Count; i++)
				{
					c = diffs[i].Apply2Previous(c);
				}
				if (!HasIdenticalElements(current, c))
				{
					System.Console.Out.WriteLine("previous = " + previous);
					System.Console.Out.WriteLine();
					System.Console.Out.WriteLine("current  = " + current);
					System.Console.Out.WriteLine("c        = " + c);
					throw new Exception("current and c are not identical.");
				}
				// check if previous == current - diffs
				IList<INode> p = current;
				for (int i_1 = diffs.Count - 1; i_1 >= 0; i_1--)
				{
					p = diffs[i_1].Apply2Current(p);
				}
				if (!HasIdenticalElements(previous, p))
				{
					System.Console.Out.WriteLine("previous = " + previous);
					System.Console.Out.WriteLine("p        = " + p);
					System.Console.Out.WriteLine();
					System.Console.Out.WriteLine("current  = " + current);
					throw new Exception("previous and p are not identical.");
				}
			}
			// combine all diffs
			Diff<byte[], INode> combined = diffs[0];
			for (int i_2 = 1; i_2 < diffs.Count; i_2++)
			{
				combined.CombinePosterior(diffs[i_2], null);
			}
			{
				// check if current == previous + combined
				IList<INode> c = combined.Apply2Previous(previous);
				if (!HasIdenticalElements(current, c))
				{
					System.Console.Out.WriteLine("previous = " + previous);
					System.Console.Out.WriteLine();
					System.Console.Out.WriteLine("current  = " + current);
					System.Console.Out.WriteLine("c        = " + c);
					throw new Exception("current and c are not identical.");
				}
				// check if previous == current - combined
				IList<INode> p = combined.Apply2Current(current);
				if (!HasIdenticalElements(previous, p))
				{
					System.Console.Out.WriteLine("previous = " + previous);
					System.Console.Out.WriteLine("p        = " + p);
					System.Console.Out.WriteLine();
					System.Console.Out.WriteLine("current  = " + current);
					throw new Exception("previous and p are not identical.");
				}
			}
			{
				for (int m_1 = 0; m_1 < n; m_1++)
				{
					INode inode = NewINode(m_1, width);
					{
						// test accessPrevious
						Diff.Container<INode> r = combined.AccessPrevious(inode.GetKey());
						INode computed;
						if (r != null)
						{
							computed = r.GetElement();
						}
						else
						{
							int i = Diff.Search(current, inode.GetKey());
							computed = i_2 < 0 ? null : current[i_2];
						}
						int j_1 = Diff.Search(previous, inode.GetKey());
						INode expected = j_1 < 0 ? null : previous[j_1];
						// must be the same object (equals is not enough)
						NUnit.Framework.Assert.IsTrue(computed == expected);
					}
					{
						// test accessCurrent
						Diff.Container<INode> r = combined.AccessCurrent(inode.GetKey());
						INode computed;
						if (r != null)
						{
							computed = r.GetElement();
						}
						else
						{
							int i = Diff.Search(previous, inode.GetKey());
							computed = i_2 < 0 ? null : previous[i_2];
						}
						int j_1 = Diff.Search(current, inode.GetKey());
						INode expected = j_1 < 0 ? null : current[j_1];
						// must be the same object (equals is not enough)
						NUnit.Framework.Assert.IsTrue(computed == expected);
					}
				}
			}
		}

		internal static bool HasIdenticalElements(IList<INode> expected, IList<INode> computed
			)
		{
			if (expected == null)
			{
				return computed == null;
			}
			if (expected.Count != computed.Count)
			{
				return false;
			}
			for (int i = 0; i < expected.Count; i++)
			{
				// must be the same object (equals is not enough)
				if (expected[i] != computed[i])
				{
					return false;
				}
			}
			return true;
		}

		internal static string ToString(INode inode)
		{
			return inode == null ? null : inode.GetLocalName() + ":" + inode.GetModificationTime
				();
		}

		internal static int FindWidth(int max)
		{
			int w = 1;
			for (long n = 10; n < max; n *= 10, w++)
			{
			}
			return w;
		}

		internal static INode NewINode(int n, int width)
		{
			byte[] name = DFSUtil.String2Bytes(string.Format("n%0" + width + "d", n));
			return new INodeDirectory(n, name, Perm, 0L);
		}

		internal static void Create(INode inode, IList<INode> current, Diff<byte[], INode
			> diff)
		{
			int i = Diff.Search(current, inode.GetKey());
			NUnit.Framework.Assert.IsTrue(i < 0);
			current.Add(-i - 1, inode);
			if (diff != null)
			{
				//test undo with 1/UNDO_TEST_P probability
				bool testUndo = Random.Next(UndoTestP) == 0;
				string before = null;
				if (testUndo)
				{
					before = diff.ToString();
				}
				int undoInfo = diff.Create(inode);
				if (testUndo)
				{
					string after = diff.ToString();
					//undo
					diff.UndoCreate(inode, undoInfo);
					AssertDiff(before, diff);
					//re-do
					diff.Create(inode);
					AssertDiff(after, diff);
				}
			}
		}

		internal static void Delete(INode inode, IList<INode> current, Diff<byte[], INode
			> diff)
		{
			int i = Diff.Search(current, inode.GetKey());
			current.Remove(i);
			if (diff != null)
			{
				//test undo with 1/UNDO_TEST_P probability
				bool testUndo = Random.Next(UndoTestP) == 0;
				string before = null;
				if (testUndo)
				{
					before = diff.ToString();
				}
				Diff.UndoInfo<INode> undoInfo = diff.Delete(inode);
				if (testUndo)
				{
					string after = diff.ToString();
					//undo
					diff.UndoDelete(inode, undoInfo);
					AssertDiff(before, diff);
					//re-do
					diff.Delete(inode);
					AssertDiff(after, diff);
				}
			}
		}

		internal static void Modify(INode inode, IList<INode> current, Diff<byte[], INode
			> diff)
		{
			int i = Diff.Search(current, inode.GetKey());
			NUnit.Framework.Assert.IsTrue(i >= 0);
			INodeDirectory oldinode = (INodeDirectory)current[i];
			INodeDirectory newinode = new INodeDirectory(oldinode, false, oldinode.GetFeatures
				());
			newinode.SetModificationTime(oldinode.GetModificationTime() + 1);
			current.Set(i, newinode);
			if (diff != null)
			{
				//test undo with 1/UNDO_TEST_P probability
				bool testUndo = Random.Next(UndoTestP) == 0;
				string before = null;
				if (testUndo)
				{
					before = diff.ToString();
				}
				Diff.UndoInfo<INode> undoInfo = diff.Modify(oldinode, newinode);
				if (testUndo)
				{
					string after = diff.ToString();
					//undo
					diff.UndoModify(oldinode, newinode, undoInfo);
					AssertDiff(before, diff);
					//re-do
					diff.Modify(oldinode, newinode);
					AssertDiff(after, diff);
				}
			}
		}

		internal static void AssertDiff(string s, Diff<byte[], INode> diff)
		{
			NUnit.Framework.Assert.AreEqual(s, diff.ToString());
		}
	}
}
