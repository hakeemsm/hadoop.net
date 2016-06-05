using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress
{
	/// <summary>Utility methods that help with writing tests covering startup progress.</summary>
	public class StartupProgressTestHelper
	{
		/// <summary>Increments a counter a certain number of times.</summary>
		/// <param name="prog">StartupProgress to increment</param>
		/// <param name="phase">Phase to increment</param>
		/// <param name="step">Step to increment</param>
		/// <param name="delta">long number of times to increment</param>
		public static void IncrementCounter(StartupProgress prog, Phase phase, Step step, 
			long delta)
		{
			StartupProgress.Counter counter = prog.GetCounter(phase, step);
			for (long i = 0; i < delta; ++i)
			{
				counter.Increment();
			}
		}

		/// <summary>Sets up StartupProgress to a state part-way through the startup sequence.
		/// 	</summary>
		/// <param name="prog">StartupProgress to set</param>
		public static void SetStartupProgressForRunningState(StartupProgress prog)
		{
			prog.BeginPhase(Phase.LoadingFsimage);
			Step loadingFsImageInodes = new Step(StepType.Inodes);
			prog.BeginStep(Phase.LoadingFsimage, loadingFsImageInodes);
			prog.SetTotal(Phase.LoadingFsimage, loadingFsImageInodes, 100L);
			IncrementCounter(prog, Phase.LoadingFsimage, loadingFsImageInodes, 100L);
			prog.EndStep(Phase.LoadingFsimage, loadingFsImageInodes);
			prog.EndPhase(Phase.LoadingFsimage);
			prog.BeginPhase(Phase.LoadingEdits);
			Step loadingEditsFile = new Step("file", 1000L);
			prog.BeginStep(Phase.LoadingEdits, loadingEditsFile);
			prog.SetTotal(Phase.LoadingEdits, loadingEditsFile, 200L);
			IncrementCounter(prog, Phase.LoadingEdits, loadingEditsFile, 100L);
		}

		/// <summary>Sets up StartupProgress to final state after startup sequence has completed.
		/// 	</summary>
		/// <param name="prog">StartupProgress to set</param>
		public static void SetStartupProgressForFinalState(StartupProgress prog)
		{
			prog.BeginPhase(Phase.LoadingFsimage);
			Step loadingFsImageInodes = new Step(StepType.Inodes);
			prog.BeginStep(Phase.LoadingFsimage, loadingFsImageInodes);
			prog.SetTotal(Phase.LoadingFsimage, loadingFsImageInodes, 100L);
			IncrementCounter(prog, Phase.LoadingFsimage, loadingFsImageInodes, 100L);
			prog.EndStep(Phase.LoadingFsimage, loadingFsImageInodes);
			prog.EndPhase(Phase.LoadingFsimage);
			prog.BeginPhase(Phase.LoadingEdits);
			Step loadingEditsFile = new Step("file", 1000L);
			prog.BeginStep(Phase.LoadingEdits, loadingEditsFile);
			prog.SetTotal(Phase.LoadingEdits, loadingEditsFile, 200L);
			IncrementCounter(prog, Phase.LoadingEdits, loadingEditsFile, 200L);
			prog.EndStep(Phase.LoadingEdits, loadingEditsFile);
			prog.EndPhase(Phase.LoadingEdits);
			prog.BeginPhase(Phase.SavingCheckpoint);
			Step savingCheckpointInodes = new Step(StepType.Inodes);
			prog.BeginStep(Phase.SavingCheckpoint, savingCheckpointInodes);
			prog.SetTotal(Phase.SavingCheckpoint, savingCheckpointInodes, 300L);
			IncrementCounter(prog, Phase.SavingCheckpoint, savingCheckpointInodes, 300L);
			prog.EndStep(Phase.SavingCheckpoint, savingCheckpointInodes);
			prog.EndPhase(Phase.SavingCheckpoint);
			prog.BeginPhase(Phase.Safemode);
			Step awaitingBlocks = new Step(StepType.AwaitingReportedBlocks);
			prog.BeginStep(Phase.Safemode, awaitingBlocks);
			prog.SetTotal(Phase.Safemode, awaitingBlocks, 400L);
			IncrementCounter(prog, Phase.Safemode, awaitingBlocks, 400L);
			prog.EndStep(Phase.Safemode, awaitingBlocks);
			prog.EndPhase(Phase.Safemode);
		}
	}
}
