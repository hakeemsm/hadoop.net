using System;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// TaskAttemptID represents the immutable and unique identifier for
	/// a task attempt.
	/// </summary>
	/// <remarks>
	/// TaskAttemptID represents the immutable and unique identifier for
	/// a task attempt. Each task attempt is one particular instance of a Map or
	/// Reduce Task identified by its TaskID.
	/// TaskAttemptID consists of 2 parts. First part is the
	/// <see cref="TaskID"/>
	/// , that this TaskAttemptID belongs to.
	/// Second part is the task attempt number. <br />
	/// An example TaskAttemptID is :
	/// <code>attempt_200707121733_0003_m_000005_0</code> , which represents the
	/// zeroth task attempt for the fifth map task in the third job
	/// running at the jobtracker started at <code>200707121733</code>.
	/// <p>
	/// Applications should never construct or parse TaskAttemptID strings
	/// , but rather use appropriate constructors or
	/// <see cref="ForName(string)"/>
	/// 
	/// method.
	/// </remarks>
	/// <seealso cref="JobID"/>
	/// <seealso cref="TaskID"/>
	public class TaskAttemptID : Org.Apache.Hadoop.Mapreduce.TaskAttemptID
	{
		/// <summary>
		/// Constructs a TaskAttemptID object from given
		/// <see cref="TaskID"/>
		/// .
		/// </summary>
		/// <param name="taskId">TaskID that this task belongs to</param>
		/// <param name="id">the task attempt number</param>
		public TaskAttemptID(TaskID taskId, int id)
			: base(taskId, id)
		{
		}

		/// <summary>Constructs a TaskId object from given parts.</summary>
		/// <param name="jtIdentifier">jobTracker identifier</param>
		/// <param name="jobId">job number</param>
		/// <param name="isMap">whether the tip is a map</param>
		/// <param name="taskId">taskId number</param>
		/// <param name="id">the task attempt number</param>
		[System.ObsoleteAttribute(@"Use TaskAttemptID(string, int, Org.Apache.Hadoop.Mapreduce.TaskType, int, int) ."
			)]
		public TaskAttemptID(string jtIdentifier, int jobId, bool isMap, int taskId, int 
			id)
			: this(jtIdentifier, jobId, isMap ? TaskType.Map : TaskType.Reduce, taskId, id)
		{
		}

		/// <summary>Constructs a TaskId object from given parts.</summary>
		/// <param name="jtIdentifier">jobTracker identifier</param>
		/// <param name="jobId">job number</param>
		/// <param name="type">the TaskType</param>
		/// <param name="taskId">taskId number</param>
		/// <param name="id">the task attempt number</param>
		public TaskAttemptID(string jtIdentifier, int jobId, TaskType type, int taskId, int
			 id)
			: this(new TaskID(jtIdentifier, jobId, type, taskId), id)
		{
		}

		public TaskAttemptID()
			: base(new TaskID(), 0)
		{
		}

		/// <summary>Downgrade a new TaskAttemptID to an old one</summary>
		/// <param name="old">the new id</param>
		/// <returns>either old or a new TaskAttemptID constructed to match old</returns>
		public static Org.Apache.Hadoop.Mapred.TaskAttemptID Downgrade(Org.Apache.Hadoop.Mapreduce.TaskAttemptID
			 old)
		{
			if (old is Org.Apache.Hadoop.Mapred.TaskAttemptID)
			{
				return (Org.Apache.Hadoop.Mapred.TaskAttemptID)old;
			}
			else
			{
				return new Org.Apache.Hadoop.Mapred.TaskAttemptID(TaskID.Downgrade(old.GetTaskID(
					)), old.GetId());
			}
		}

		public override TaskID GetTaskID()
		{
			return (TaskID)base.GetTaskID();
		}

		public override JobID GetJobID()
		{
			return (JobID)base.GetJobID();
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public static Org.Apache.Hadoop.Mapred.TaskAttemptID Read(DataInput @in)
		{
			Org.Apache.Hadoop.Mapred.TaskAttemptID taskId = new Org.Apache.Hadoop.Mapred.TaskAttemptID
				();
			taskId.ReadFields(@in);
			return taskId;
		}

		/// <summary>Construct a TaskAttemptID object from given string</summary>
		/// <returns>constructed TaskAttemptID object or null if the given String is null</returns>
		/// <exception cref="System.ArgumentException">if the given string is malformed</exception>
		public static Org.Apache.Hadoop.Mapreduce.TaskAttemptID ForName(string str)
		{
			return (Org.Apache.Hadoop.Mapred.TaskAttemptID)Org.Apache.Hadoop.Mapreduce.TaskAttemptID
				.ForName(str);
		}

		/// <summary>Returns a regex pattern which matches task attempt IDs.</summary>
		/// <remarks>
		/// Returns a regex pattern which matches task attempt IDs. Arguments can
		/// be given null, in which case that part of the regex will be generic.
		/// For example to obtain a regex matching <i>all task attempt IDs</i>
		/// of <i>any jobtracker</i>, in <i>any job</i>, of the <i>first
		/// map task</i>, we would use :
		/// <pre>
		/// TaskAttemptID.getTaskAttemptIDsPattern(null, null, true, 1, null);
		/// </pre>
		/// which will return :
		/// <pre> "attempt_[^_]*_[0-9]*_m_000001_[0-9]*" </pre>
		/// </remarks>
		/// <param name="jtIdentifier">jobTracker identifier, or null</param>
		/// <param name="jobId">job number, or null</param>
		/// <param name="isMap">whether the tip is a map, or null</param>
		/// <param name="taskId">taskId number, or null</param>
		/// <param name="attemptId">the task attempt number, or null</param>
		/// <returns>a regex pattern matching TaskAttemptIDs</returns>
		[Obsolete]
		public static string GetTaskAttemptIDsPattern(string jtIdentifier, int jobId, bool
			 isMap, int taskId, int attemptId)
		{
			return GetTaskAttemptIDsPattern(jtIdentifier, jobId, isMap ? TaskType.Map : TaskType
				.Reduce, taskId, attemptId);
		}

		/// <summary>Returns a regex pattern which matches task attempt IDs.</summary>
		/// <remarks>
		/// Returns a regex pattern which matches task attempt IDs. Arguments can
		/// be given null, in which case that part of the regex will be generic.
		/// For example to obtain a regex matching <i>all task attempt IDs</i>
		/// of <i>any jobtracker</i>, in <i>any job</i>, of the <i>first
		/// map task</i>, we would use :
		/// <pre>
		/// TaskAttemptID.getTaskAttemptIDsPattern(null, null, TaskType.MAP, 1, null);
		/// </pre>
		/// which will return :
		/// <pre> "attempt_[^_]*_[0-9]*_m_000001_[0-9]*" </pre>
		/// </remarks>
		/// <param name="jtIdentifier">jobTracker identifier, or null</param>
		/// <param name="jobId">job number, or null</param>
		/// <param name="type">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.TaskType"/>
		/// 
		/// </param>
		/// <param name="taskId">taskId number, or null</param>
		/// <param name="attemptId">the task attempt number, or null</param>
		/// <returns>a regex pattern matching TaskAttemptIDs</returns>
		[Obsolete]
		public static string GetTaskAttemptIDsPattern(string jtIdentifier, int jobId, TaskType
			 type, int taskId, int attemptId)
		{
			StringBuilder builder = new StringBuilder(Attempt).Append(Separator);
			builder.Append(GetTaskAttemptIDsPatternWOPrefix(jtIdentifier, jobId, type, taskId
				, attemptId));
			return builder.ToString();
		}

		[Obsolete]
		internal static StringBuilder GetTaskAttemptIDsPatternWOPrefix(string jtIdentifier
			, int jobId, TaskType type, int taskId, int attemptId)
		{
			StringBuilder builder = new StringBuilder();
			builder.Append(TaskID.GetTaskIDsPatternWOPrefix(jtIdentifier, jobId, type, taskId
				)).Append(Separator).Append(attemptId != null ? attemptId : "[0-9]*");
			return builder;
		}
	}
}
