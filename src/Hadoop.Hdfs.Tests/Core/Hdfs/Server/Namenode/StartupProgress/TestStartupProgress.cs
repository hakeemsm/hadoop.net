using System.Collections.Generic;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress
{
	public class TestStartupProgress
	{
		private StartupProgress startupProgress;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			startupProgress = new StartupProgress();
		}

		public virtual void TestCounter()
		{
			startupProgress.BeginPhase(Phase.LoadingFsimage);
			Step loadingFsImageInodes = new Step(StepType.Inodes);
			startupProgress.BeginStep(Phase.LoadingFsimage, loadingFsImageInodes);
			StartupProgressTestHelper.IncrementCounter(startupProgress, Phase.LoadingFsimage, 
				loadingFsImageInodes, 100L);
			startupProgress.EndStep(Phase.LoadingFsimage, loadingFsImageInodes);
			Step loadingFsImageDelegationKeys = new Step(StepType.DelegationKeys);
			startupProgress.BeginStep(Phase.LoadingFsimage, loadingFsImageDelegationKeys);
			StartupProgressTestHelper.IncrementCounter(startupProgress, Phase.LoadingFsimage, 
				loadingFsImageDelegationKeys, 200L);
			startupProgress.EndStep(Phase.LoadingFsimage, loadingFsImageDelegationKeys);
			startupProgress.EndPhase(Phase.LoadingFsimage);
			startupProgress.BeginPhase(Phase.LoadingEdits);
			Step loadingEditsFile = new Step("file", 1000L);
			startupProgress.BeginStep(Phase.LoadingEdits, loadingEditsFile);
			StartupProgressTestHelper.IncrementCounter(startupProgress, Phase.LoadingEdits, loadingEditsFile
				, 5000L);
			StartupProgressView view = startupProgress.CreateView();
			NUnit.Framework.Assert.IsNotNull(view);
			NUnit.Framework.Assert.AreEqual(100L, view.GetCount(Phase.LoadingFsimage, loadingFsImageInodes
				));
			NUnit.Framework.Assert.AreEqual(200L, view.GetCount(Phase.LoadingFsimage, loadingFsImageDelegationKeys
				));
			NUnit.Framework.Assert.AreEqual(5000L, view.GetCount(Phase.LoadingEdits, loadingEditsFile
				));
			NUnit.Framework.Assert.AreEqual(0L, view.GetCount(Phase.SavingCheckpoint, new Step
				(StepType.Inodes)));
			// Increment a counter again and check that the existing view was not
			// modified, but a new view shows the updated value.
			StartupProgressTestHelper.IncrementCounter(startupProgress, Phase.LoadingEdits, loadingEditsFile
				, 1000L);
			startupProgress.EndStep(Phase.LoadingEdits, loadingEditsFile);
			startupProgress.EndPhase(Phase.LoadingEdits);
			NUnit.Framework.Assert.AreEqual(5000L, view.GetCount(Phase.LoadingEdits, loadingEditsFile
				));
			view = startupProgress.CreateView();
			NUnit.Framework.Assert.IsNotNull(view);
			NUnit.Framework.Assert.AreEqual(6000L, view.GetCount(Phase.LoadingEdits, loadingEditsFile
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestElapsedTime()
		{
			startupProgress.BeginPhase(Phase.LoadingFsimage);
			Step loadingFsImageInodes = new Step(StepType.Inodes);
			startupProgress.BeginStep(Phase.LoadingFsimage, loadingFsImageInodes);
			Sharpen.Thread.Sleep(50L);
			// brief sleep to fake elapsed time
			startupProgress.EndStep(Phase.LoadingFsimage, loadingFsImageInodes);
			Step loadingFsImageDelegationKeys = new Step(StepType.DelegationKeys);
			startupProgress.BeginStep(Phase.LoadingFsimage, loadingFsImageDelegationKeys);
			Sharpen.Thread.Sleep(50L);
			// brief sleep to fake elapsed time
			startupProgress.EndStep(Phase.LoadingFsimage, loadingFsImageDelegationKeys);
			startupProgress.EndPhase(Phase.LoadingFsimage);
			startupProgress.BeginPhase(Phase.LoadingEdits);
			Step loadingEditsFile = new Step("file", 1000L);
			startupProgress.BeginStep(Phase.LoadingEdits, loadingEditsFile);
			startupProgress.SetTotal(Phase.LoadingEdits, loadingEditsFile, 10000L);
			StartupProgressTestHelper.IncrementCounter(startupProgress, Phase.LoadingEdits, loadingEditsFile
				, 5000L);
			Sharpen.Thread.Sleep(50L);
			// brief sleep to fake elapsed time
			StartupProgressView view = startupProgress.CreateView();
			NUnit.Framework.Assert.IsNotNull(view);
			NUnit.Framework.Assert.IsTrue(view.GetElapsedTime() > 0);
			NUnit.Framework.Assert.IsTrue(view.GetElapsedTime(Phase.LoadingFsimage) > 0);
			NUnit.Framework.Assert.IsTrue(view.GetElapsedTime(Phase.LoadingFsimage, loadingFsImageInodes
				) > 0);
			NUnit.Framework.Assert.IsTrue(view.GetElapsedTime(Phase.LoadingFsimage, loadingFsImageDelegationKeys
				) > 0);
			NUnit.Framework.Assert.IsTrue(view.GetElapsedTime(Phase.LoadingEdits) > 0);
			NUnit.Framework.Assert.IsTrue(view.GetElapsedTime(Phase.LoadingEdits, loadingEditsFile
				) > 0);
			NUnit.Framework.Assert.IsTrue(view.GetElapsedTime(Phase.SavingCheckpoint) == 0);
			NUnit.Framework.Assert.IsTrue(view.GetElapsedTime(Phase.SavingCheckpoint, new Step
				(StepType.Inodes)) == 0);
			// Brief sleep, then check that completed phases/steps have the same elapsed
			// time, but running phases/steps have updated elapsed time.
			long totalTime = view.GetElapsedTime();
			long loadingFsImageTime = view.GetElapsedTime(Phase.LoadingFsimage);
			long loadingFsImageInodesTime = view.GetElapsedTime(Phase.LoadingFsimage, loadingFsImageInodes
				);
			long loadingFsImageDelegationKeysTime = view.GetElapsedTime(Phase.LoadingFsimage, 
				loadingFsImageInodes);
			long loadingEditsTime = view.GetElapsedTime(Phase.LoadingEdits);
			long loadingEditsFileTime = view.GetElapsedTime(Phase.LoadingEdits, loadingEditsFile
				);
			Sharpen.Thread.Sleep(50L);
			NUnit.Framework.Assert.IsTrue(totalTime < view.GetElapsedTime());
			NUnit.Framework.Assert.AreEqual(loadingFsImageTime, view.GetElapsedTime(Phase.LoadingFsimage
				));
			NUnit.Framework.Assert.AreEqual(loadingFsImageInodesTime, view.GetElapsedTime(Phase
				.LoadingFsimage, loadingFsImageInodes));
			NUnit.Framework.Assert.IsTrue(loadingEditsTime < view.GetElapsedTime(Phase.LoadingEdits
				));
			NUnit.Framework.Assert.IsTrue(loadingEditsFileTime < view.GetElapsedTime(Phase.LoadingEdits
				, loadingEditsFile));
		}

		public virtual void TestFrozenAfterStartupCompletes()
		{
			// Do some updates and counter increments.
			startupProgress.BeginPhase(Phase.LoadingFsimage);
			startupProgress.SetFile(Phase.LoadingFsimage, "file1");
			startupProgress.SetSize(Phase.LoadingFsimage, 1000L);
			Step step = new Step(StepType.Inodes);
			startupProgress.BeginStep(Phase.LoadingFsimage, step);
			startupProgress.SetTotal(Phase.LoadingFsimage, step, 10000L);
			StartupProgressTestHelper.IncrementCounter(startupProgress, Phase.LoadingFsimage, 
				step, 100L);
			startupProgress.EndStep(Phase.LoadingFsimage, step);
			startupProgress.EndPhase(Phase.LoadingFsimage);
			// Force completion of phases, so that entire startup process is completed.
			foreach (Phase phase in EnumSet.AllOf<Phase>())
			{
				if (startupProgress.GetStatus(phase) != Status.Complete)
				{
					startupProgress.BeginPhase(phase);
					startupProgress.EndPhase(phase);
				}
			}
			StartupProgressView before = startupProgress.CreateView();
			// Attempt more updates and counter increments.
			startupProgress.BeginPhase(Phase.LoadingFsimage);
			startupProgress.SetFile(Phase.LoadingFsimage, "file2");
			startupProgress.SetSize(Phase.LoadingFsimage, 2000L);
			startupProgress.BeginStep(Phase.LoadingFsimage, step);
			startupProgress.SetTotal(Phase.LoadingFsimage, step, 20000L);
			StartupProgressTestHelper.IncrementCounter(startupProgress, Phase.LoadingFsimage, 
				step, 100L);
			startupProgress.EndStep(Phase.LoadingFsimage, step);
			startupProgress.EndPhase(Phase.LoadingFsimage);
			// Also attempt a whole new step that wasn't used last time.
			startupProgress.BeginPhase(Phase.LoadingEdits);
			Step newStep = new Step("file1");
			startupProgress.BeginStep(Phase.LoadingEdits, newStep);
			StartupProgressTestHelper.IncrementCounter(startupProgress, Phase.LoadingEdits, newStep
				, 100L);
			startupProgress.EndStep(Phase.LoadingEdits, newStep);
			startupProgress.EndPhase(Phase.LoadingEdits);
			StartupProgressView after = startupProgress.CreateView();
			// Expect that data was frozen after completion of entire startup process, so
			// second set of updates and counter increments should have had no effect.
			NUnit.Framework.Assert.AreEqual(before.GetCount(Phase.LoadingFsimage), after.GetCount
				(Phase.LoadingFsimage));
			NUnit.Framework.Assert.AreEqual(before.GetCount(Phase.LoadingFsimage, step), after
				.GetCount(Phase.LoadingFsimage, step));
			NUnit.Framework.Assert.AreEqual(before.GetElapsedTime(), after.GetElapsedTime());
			NUnit.Framework.Assert.AreEqual(before.GetElapsedTime(Phase.LoadingFsimage), after
				.GetElapsedTime(Phase.LoadingFsimage));
			NUnit.Framework.Assert.AreEqual(before.GetElapsedTime(Phase.LoadingFsimage, step)
				, after.GetElapsedTime(Phase.LoadingFsimage, step));
			NUnit.Framework.Assert.AreEqual(before.GetFile(Phase.LoadingFsimage), after.GetFile
				(Phase.LoadingFsimage));
			NUnit.Framework.Assert.AreEqual(before.GetSize(Phase.LoadingFsimage), after.GetSize
				(Phase.LoadingFsimage));
			NUnit.Framework.Assert.AreEqual(before.GetTotal(Phase.LoadingFsimage), after.GetTotal
				(Phase.LoadingFsimage));
			NUnit.Framework.Assert.AreEqual(before.GetTotal(Phase.LoadingFsimage, step), after
				.GetTotal(Phase.LoadingFsimage, step));
			NUnit.Framework.Assert.IsFalse(after.GetSteps(Phase.LoadingEdits).GetEnumerator()
				.HasNext());
		}

		public virtual void TestInitialState()
		{
			StartupProgressView view = startupProgress.CreateView();
			NUnit.Framework.Assert.IsNotNull(view);
			NUnit.Framework.Assert.AreEqual(0L, view.GetElapsedTime());
			NUnit.Framework.Assert.AreEqual(0.0f, view.GetPercentComplete(), 0.001f);
			IList<Phase> phases = new AList<Phase>();
			foreach (Phase phase in view.GetPhases())
			{
				phases.AddItem(phase);
				NUnit.Framework.Assert.AreEqual(0L, view.GetElapsedTime(phase));
				NUnit.Framework.Assert.IsNull(view.GetFile(phase));
				NUnit.Framework.Assert.AreEqual(0.0f, view.GetPercentComplete(phase), 0.001f);
				NUnit.Framework.Assert.AreEqual(long.MinValue, view.GetSize(phase));
				NUnit.Framework.Assert.AreEqual(Status.Pending, view.GetStatus(phase));
				NUnit.Framework.Assert.AreEqual(0L, view.GetTotal(phase));
				foreach (Step step in view.GetSteps(phase))
				{
					NUnit.Framework.Assert.Fail(string.Format("unexpected step %s in phase %s at initial state"
						, step, phase));
				}
			}
			Assert.AssertArrayEquals(Sharpen.Collections.ToArray(EnumSet.AllOf<Phase>()), Sharpen.Collections.ToArray
				(phases));
		}

		public virtual void TestPercentComplete()
		{
			startupProgress.BeginPhase(Phase.LoadingFsimage);
			Step loadingFsImageInodes = new Step(StepType.Inodes);
			startupProgress.BeginStep(Phase.LoadingFsimage, loadingFsImageInodes);
			startupProgress.SetTotal(Phase.LoadingFsimage, loadingFsImageInodes, 1000L);
			StartupProgressTestHelper.IncrementCounter(startupProgress, Phase.LoadingFsimage, 
				loadingFsImageInodes, 100L);
			Step loadingFsImageDelegationKeys = new Step(StepType.DelegationKeys);
			startupProgress.BeginStep(Phase.LoadingFsimage, loadingFsImageDelegationKeys);
			startupProgress.SetTotal(Phase.LoadingFsimage, loadingFsImageDelegationKeys, 800L
				);
			StartupProgressTestHelper.IncrementCounter(startupProgress, Phase.LoadingFsimage, 
				loadingFsImageDelegationKeys, 200L);
			startupProgress.BeginPhase(Phase.LoadingEdits);
			Step loadingEditsFile = new Step("file", 1000L);
			startupProgress.BeginStep(Phase.LoadingEdits, loadingEditsFile);
			startupProgress.SetTotal(Phase.LoadingEdits, loadingEditsFile, 10000L);
			StartupProgressTestHelper.IncrementCounter(startupProgress, Phase.LoadingEdits, loadingEditsFile
				, 5000L);
			StartupProgressView view = startupProgress.CreateView();
			NUnit.Framework.Assert.IsNotNull(view);
			NUnit.Framework.Assert.AreEqual(0.167f, view.GetPercentComplete(), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.167f, view.GetPercentComplete(Phase.LoadingFsimage
				), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.10f, view.GetPercentComplete(Phase.LoadingFsimage
				, loadingFsImageInodes), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.25f, view.GetPercentComplete(Phase.LoadingFsimage
				, loadingFsImageDelegationKeys), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.5f, view.GetPercentComplete(Phase.LoadingEdits)
				, 0.001f);
			NUnit.Framework.Assert.AreEqual(0.5f, view.GetPercentComplete(Phase.LoadingEdits, 
				loadingEditsFile), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.0f, view.GetPercentComplete(Phase.SavingCheckpoint
				), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.0f, view.GetPercentComplete(Phase.SavingCheckpoint
				, new Step(StepType.Inodes)), 0.001f);
			// End steps/phases, and confirm that they jump to 100% completion.
			startupProgress.EndStep(Phase.LoadingFsimage, loadingFsImageInodes);
			startupProgress.EndStep(Phase.LoadingFsimage, loadingFsImageDelegationKeys);
			startupProgress.EndPhase(Phase.LoadingFsimage);
			startupProgress.EndStep(Phase.LoadingEdits, loadingEditsFile);
			startupProgress.EndPhase(Phase.LoadingEdits);
			view = startupProgress.CreateView();
			NUnit.Framework.Assert.IsNotNull(view);
			NUnit.Framework.Assert.AreEqual(0.5f, view.GetPercentComplete(), 0.001f);
			NUnit.Framework.Assert.AreEqual(1.0f, view.GetPercentComplete(Phase.LoadingFsimage
				), 0.001f);
			NUnit.Framework.Assert.AreEqual(1.0f, view.GetPercentComplete(Phase.LoadingFsimage
				, loadingFsImageInodes), 0.001f);
			NUnit.Framework.Assert.AreEqual(1.0f, view.GetPercentComplete(Phase.LoadingFsimage
				, loadingFsImageDelegationKeys), 0.001f);
			NUnit.Framework.Assert.AreEqual(1.0f, view.GetPercentComplete(Phase.LoadingEdits)
				, 0.001f);
			NUnit.Framework.Assert.AreEqual(1.0f, view.GetPercentComplete(Phase.LoadingEdits, 
				loadingEditsFile), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.0f, view.GetPercentComplete(Phase.SavingCheckpoint
				), 0.001f);
			NUnit.Framework.Assert.AreEqual(0.0f, view.GetPercentComplete(Phase.SavingCheckpoint
				, new Step(StepType.Inodes)), 0.001f);
		}

		public virtual void TestStatus()
		{
			startupProgress.BeginPhase(Phase.LoadingFsimage);
			startupProgress.EndPhase(Phase.LoadingFsimage);
			startupProgress.BeginPhase(Phase.LoadingEdits);
			StartupProgressView view = startupProgress.CreateView();
			NUnit.Framework.Assert.IsNotNull(view);
			NUnit.Framework.Assert.AreEqual(Status.Complete, view.GetStatus(Phase.LoadingFsimage
				));
			NUnit.Framework.Assert.AreEqual(Status.Running, view.GetStatus(Phase.LoadingEdits
				));
			NUnit.Framework.Assert.AreEqual(Status.Pending, view.GetStatus(Phase.SavingCheckpoint
				));
		}

		public virtual void TestStepSequence()
		{
			// Test that steps are returned in the correct sort order (by file and then
			// sequence number) by starting a few steps in a randomly shuffled order and
			// then asserting that they are returned in the expected order.
			Step[] expectedSteps = new Step[] { new Step(StepType.Inodes, "file1"), new Step(
				StepType.DelegationKeys, "file1"), new Step(StepType.Inodes, "file2"), new Step(
				StepType.DelegationKeys, "file2"), new Step(StepType.Inodes, "file3"), new Step(
				StepType.DelegationKeys, "file3") };
			IList<Step> shuffledSteps = new AList<Step>(Arrays.AsList(expectedSteps));
			Sharpen.Collections.Shuffle(shuffledSteps);
			startupProgress.BeginPhase(Phase.SavingCheckpoint);
			foreach (Step step in shuffledSteps)
			{
				startupProgress.BeginStep(Phase.SavingCheckpoint, step);
			}
			IList<Step> actualSteps = new AList<Step>(expectedSteps.Length);
			StartupProgressView view = startupProgress.CreateView();
			NUnit.Framework.Assert.IsNotNull(view);
			foreach (Step step_1 in view.GetSteps(Phase.SavingCheckpoint))
			{
				actualSteps.AddItem(step_1);
			}
			Assert.AssertArrayEquals(expectedSteps, Sharpen.Collections.ToArray(actualSteps));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestThreadSafety()
		{
			// Test for thread safety by starting multiple threads that mutate the same
			// StartupProgress instance in various ways.  We expect no internal
			// corruption of data structures and no lost updates on counter increments.
			int numThreads = 100;
			// Data tables used by each thread to determine values to pass to APIs.
			Phase[] phases = new Phase[] { Phase.LoadingFsimage, Phase.LoadingFsimage, Phase.
				LoadingEdits, Phase.LoadingEdits };
			Step[] steps = new Step[] { new Step(StepType.Inodes), new Step(StepType.DelegationKeys
				), new Step(StepType.Inodes), new Step(StepType.DelegationKeys) };
			string[] files = new string[] { "file1", "file1", "file2", "file2" };
			long[] sizes = new long[] { 1000L, 1000L, 2000L, 2000L };
			long[] totals = new long[] { 10000L, 20000L, 30000L, 40000L };
			ExecutorService exec = Executors.NewFixedThreadPool(numThreads);
			try
			{
				for (int i = 0; i < numThreads; ++i)
				{
					Phase phase = phases[i % phases.Length];
					Step step = steps[i % steps.Length];
					string file = files[i % files.Length];
					long size = sizes[i % sizes.Length];
					long total = totals[i % totals.Length];
					exec.Submit(new _Callable_369(this, phase, file, size, step, total));
				}
			}
			finally
			{
				exec.Shutdown();
				NUnit.Framework.Assert.IsTrue(exec.AwaitTermination(10000L, TimeUnit.Milliseconds
					));
			}
			StartupProgressView view = startupProgress.CreateView();
			NUnit.Framework.Assert.IsNotNull(view);
			NUnit.Framework.Assert.AreEqual("file1", view.GetFile(Phase.LoadingFsimage));
			NUnit.Framework.Assert.AreEqual(1000L, view.GetSize(Phase.LoadingFsimage));
			NUnit.Framework.Assert.AreEqual(10000L, view.GetTotal(Phase.LoadingFsimage, new Step
				(StepType.Inodes)));
			NUnit.Framework.Assert.AreEqual(2500L, view.GetCount(Phase.LoadingFsimage, new Step
				(StepType.Inodes)));
			NUnit.Framework.Assert.AreEqual(20000L, view.GetTotal(Phase.LoadingFsimage, new Step
				(StepType.DelegationKeys)));
			NUnit.Framework.Assert.AreEqual(2500L, view.GetCount(Phase.LoadingFsimage, new Step
				(StepType.DelegationKeys)));
			NUnit.Framework.Assert.AreEqual("file2", view.GetFile(Phase.LoadingEdits));
			NUnit.Framework.Assert.AreEqual(2000L, view.GetSize(Phase.LoadingEdits));
			NUnit.Framework.Assert.AreEqual(30000L, view.GetTotal(Phase.LoadingEdits, new Step
				(StepType.Inodes)));
			NUnit.Framework.Assert.AreEqual(2500L, view.GetCount(Phase.LoadingEdits, new Step
				(StepType.Inodes)));
			NUnit.Framework.Assert.AreEqual(40000L, view.GetTotal(Phase.LoadingEdits, new Step
				(StepType.DelegationKeys)));
			NUnit.Framework.Assert.AreEqual(2500L, view.GetCount(Phase.LoadingEdits, new Step
				(StepType.DelegationKeys)));
		}

		private sealed class _Callable_369 : Callable<Void>
		{
			public _Callable_369(TestStartupProgress _enclosing, Phase phase, string file, long
				 size, Step step, long total)
			{
				this._enclosing = _enclosing;
				this.phase = phase;
				this.file = file;
				this.size = size;
				this.step = step;
				this.total = total;
			}

			public Void Call()
			{
				this._enclosing.startupProgress.BeginPhase(phase);
				this._enclosing.startupProgress.SetFile(phase, file);
				this._enclosing.startupProgress.SetSize(phase, size);
				this._enclosing.startupProgress.SetTotal(phase, step, total);
				StartupProgressTestHelper.IncrementCounter(this._enclosing.startupProgress, phase
					, step, 100L);
				this._enclosing.startupProgress.EndStep(phase, step);
				this._enclosing.startupProgress.EndPhase(phase);
				return null;
			}

			private readonly TestStartupProgress _enclosing;

			private readonly Phase phase;

			private readonly string file;

			private readonly long size;

			private readonly Step step;

			private readonly long total;
		}

		public virtual void TestTotal()
		{
			startupProgress.BeginPhase(Phase.LoadingFsimage);
			Step loadingFsImageInodes = new Step(StepType.Inodes);
			startupProgress.BeginStep(Phase.LoadingFsimage, loadingFsImageInodes);
			startupProgress.SetTotal(Phase.LoadingFsimage, loadingFsImageInodes, 1000L);
			startupProgress.EndStep(Phase.LoadingFsimage, loadingFsImageInodes);
			Step loadingFsImageDelegationKeys = new Step(StepType.DelegationKeys);
			startupProgress.BeginStep(Phase.LoadingFsimage, loadingFsImageDelegationKeys);
			startupProgress.SetTotal(Phase.LoadingFsimage, loadingFsImageDelegationKeys, 800L
				);
			startupProgress.EndStep(Phase.LoadingFsimage, loadingFsImageDelegationKeys);
			startupProgress.EndPhase(Phase.LoadingFsimage);
			startupProgress.BeginPhase(Phase.LoadingEdits);
			Step loadingEditsFile = new Step("file", 1000L);
			startupProgress.BeginStep(Phase.LoadingEdits, loadingEditsFile);
			startupProgress.SetTotal(Phase.LoadingEdits, loadingEditsFile, 10000L);
			startupProgress.EndStep(Phase.LoadingEdits, loadingEditsFile);
			startupProgress.EndPhase(Phase.LoadingEdits);
			StartupProgressView view = startupProgress.CreateView();
			NUnit.Framework.Assert.IsNotNull(view);
			NUnit.Framework.Assert.AreEqual(1000L, view.GetTotal(Phase.LoadingFsimage, loadingFsImageInodes
				));
			NUnit.Framework.Assert.AreEqual(800L, view.GetTotal(Phase.LoadingFsimage, loadingFsImageDelegationKeys
				));
			NUnit.Framework.Assert.AreEqual(10000L, view.GetTotal(Phase.LoadingEdits, loadingEditsFile
				));
		}
	}
}
