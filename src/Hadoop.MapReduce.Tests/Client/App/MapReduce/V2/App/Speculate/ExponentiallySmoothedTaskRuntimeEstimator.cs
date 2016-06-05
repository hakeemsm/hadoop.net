using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Speculate
{
	/// <summary>
	/// This estimator exponentially smooths the rate of progress versus wallclock
	/// time.
	/// </summary>
	/// <remarks>
	/// This estimator exponentially smooths the rate of progress versus wallclock
	/// time.  Conceivably we could write an estimator that smooths time per
	/// unit progress, and get different results.
	/// </remarks>
	public class ExponentiallySmoothedTaskRuntimeEstimator : StartEndTimesBase
	{
		private readonly ConcurrentMap<TaskAttemptId, AtomicReference<ExponentiallySmoothedTaskRuntimeEstimator.EstimateVector
			>> estimates = new ConcurrentHashMap<TaskAttemptId, AtomicReference<ExponentiallySmoothedTaskRuntimeEstimator.EstimateVector
			>>();

		private ExponentiallySmoothedTaskRuntimeEstimator.SmoothedValue smoothedValue;

		private long lambda;

		public enum SmoothedValue
		{
			Rate,
			TimePerUnitProgress
		}

		internal ExponentiallySmoothedTaskRuntimeEstimator(long lambda, ExponentiallySmoothedTaskRuntimeEstimator.SmoothedValue
			 smoothedValue)
			: base()
		{
			this.smoothedValue = smoothedValue;
			this.lambda = lambda;
		}

		public ExponentiallySmoothedTaskRuntimeEstimator()
			: base()
		{
		}

		private class EstimateVector
		{
			internal readonly double value;

			internal readonly float basedOnProgress;

			internal readonly long atTime;

			internal EstimateVector(ExponentiallySmoothedTaskRuntimeEstimator _enclosing, double
				 value, float basedOnProgress, long atTime)
			{
				this._enclosing = _enclosing;
				// immutable
				this.value = value;
				this.basedOnProgress = basedOnProgress;
				this.atTime = atTime;
			}

			internal virtual ExponentiallySmoothedTaskRuntimeEstimator.EstimateVector Incorporate
				(float newProgress, long newAtTime)
			{
				if (newAtTime <= this.atTime || newProgress < this.basedOnProgress)
				{
					return this;
				}
				double oldWeighting = this.value < 0.0 ? 0.0 : Math.Exp(((double)(newAtTime - this
					.atTime)) / this._enclosing.lambda);
				double newRead = (newProgress - this.basedOnProgress) / (newAtTime - this.atTime);
				if (this._enclosing.smoothedValue == ExponentiallySmoothedTaskRuntimeEstimator.SmoothedValue
					.TimePerUnitProgress)
				{
					newRead = 1.0 / newRead;
				}
				return new ExponentiallySmoothedTaskRuntimeEstimator.EstimateVector(this, this.value
					 * oldWeighting + newRead * (1.0 - oldWeighting), newProgress, newAtTime);
			}

			private readonly ExponentiallySmoothedTaskRuntimeEstimator _enclosing;
		}

		private void IncorporateReading(TaskAttemptId attemptID, float newProgress, long 
			newTime)
		{
			//TODO: Refactor this method, it seems more complicated than necessary.
			AtomicReference<ExponentiallySmoothedTaskRuntimeEstimator.EstimateVector> vectorRef
				 = estimates[attemptID];
			if (vectorRef == null)
			{
				estimates.PutIfAbsent(attemptID, new AtomicReference<ExponentiallySmoothedTaskRuntimeEstimator.EstimateVector
					>(null));
				IncorporateReading(attemptID, newProgress, newTime);
				return;
			}
			ExponentiallySmoothedTaskRuntimeEstimator.EstimateVector oldVector = vectorRef.Get
				();
			if (oldVector == null)
			{
				if (vectorRef.CompareAndSet(null, new ExponentiallySmoothedTaskRuntimeEstimator.EstimateVector
					(this, -1.0, 0.0F, long.MinValue)))
				{
					return;
				}
				IncorporateReading(attemptID, newProgress, newTime);
				return;
			}
			while (!vectorRef.CompareAndSet(oldVector, oldVector.Incorporate(newProgress, newTime
				)))
			{
				oldVector = vectorRef.Get();
			}
		}

		private ExponentiallySmoothedTaskRuntimeEstimator.EstimateVector GetEstimateVector
			(TaskAttemptId attemptID)
		{
			AtomicReference<ExponentiallySmoothedTaskRuntimeEstimator.EstimateVector> vectorRef
				 = estimates[attemptID];
			if (vectorRef == null)
			{
				return null;
			}
			return vectorRef.Get();
		}

		public override void Contextualize(Configuration conf, AppContext context)
		{
			base.Contextualize(conf, context);
			lambda = conf.GetLong(MRJobConfig.MrAmTaskEstimatorSmoothLambdaMs, MRJobConfig.DefaultMrAmTaskEstimatorSmoothLambdaMs
				);
			smoothedValue = conf.GetBoolean(MRJobConfig.MrAmTaskEstimatorExponentialRateEnable
				, true) ? ExponentiallySmoothedTaskRuntimeEstimator.SmoothedValue.Rate : ExponentiallySmoothedTaskRuntimeEstimator.SmoothedValue
				.TimePerUnitProgress;
		}

		public override long EstimatedRuntime(TaskAttemptId id)
		{
			long startTime = startTimes[id];
			if (startTime == null)
			{
				return -1L;
			}
			ExponentiallySmoothedTaskRuntimeEstimator.EstimateVector vector = GetEstimateVector
				(id);
			if (vector == null)
			{
				return -1L;
			}
			long sunkTime = vector.atTime - startTime;
			double value = vector.value;
			float progress = vector.basedOnProgress;
			if (value == 0)
			{
				return -1L;
			}
			double rate = smoothedValue == ExponentiallySmoothedTaskRuntimeEstimator.SmoothedValue
				.Rate ? value : 1.0 / value;
			if (rate == 0.0)
			{
				return -1L;
			}
			double remainingTime = (1.0 - progress) / rate;
			return sunkTime + (long)remainingTime;
		}

		public override long RuntimeEstimateVariance(TaskAttemptId id)
		{
			return -1L;
		}

		public override void UpdateAttempt(TaskAttemptStatusUpdateEvent.TaskAttemptStatus
			 status, long timestamp)
		{
			base.UpdateAttempt(status, timestamp);
			TaskAttemptId attemptID = status.id;
			float progress = status.progress;
			IncorporateReading(attemptID, progress, timestamp);
		}
	}
}
