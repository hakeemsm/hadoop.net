using System;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// TaskID represents the immutable and unique identifier for
	/// a Map or Reduce Task.
	/// </summary>
	/// <remarks>
	/// TaskID represents the immutable and unique identifier for
	/// a Map or Reduce Task. Each TaskID encompasses multiple attempts made to
	/// execute the Map or Reduce Task, each of which are uniquely indentified by
	/// their TaskAttemptID.
	/// TaskID consists of 3 parts. First part is the
	/// <see cref="JobID"/>
	/// , that this
	/// TaskInProgress belongs to. Second part of the TaskID is either 'm' or 'r'
	/// representing whether the task is a map task or a reduce task.
	/// And the third part is the task number. <br />
	/// An example TaskID is :
	/// <code>task_200707121733_0003_m_000005</code> , which represents the
	/// fifth map task in the third job running at the jobtracker
	/// started at <code>200707121733</code>.
	/// <p>
	/// Applications should never construct or parse TaskID strings
	/// , but rather use appropriate constructors or
	/// <see cref="ForName(string)"/>
	/// 
	/// method.
	/// </remarks>
	/// <seealso cref="JobID"/>
	/// <seealso cref="TaskAttemptID"/>
	public class TaskID : Org.Apache.Hadoop.Mapreduce.TaskID
	{
		/// <summary>
		/// Constructs a TaskID object from given
		/// <see cref="JobID"/>
		/// .
		/// </summary>
		/// <param name="jobId">JobID that this tip belongs to</param>
		/// <param name="isMap">whether the tip is a map</param>
		/// <param name="id">the tip number</param>
		[System.ObsoleteAttribute(@"Use TaskID(string, int, Org.Apache.Hadoop.Mapreduce.TaskType, int)"
			)]
		public TaskID(JobID jobId, bool isMap, int id)
			: this(jobId, isMap ? TaskType.Map : TaskType.Reduce, id)
		{
		}

		/// <summary>Constructs a TaskInProgressId object from given parts.</summary>
		/// <param name="jtIdentifier">jobTracker identifier</param>
		/// <param name="jobId">job number</param>
		/// <param name="isMap">whether the tip is a map</param>
		/// <param name="id">the tip number</param>
		[System.ObsoleteAttribute(@"Use TaskID(Org.Apache.Hadoop.Mapreduce.JobID, Org.Apache.Hadoop.Mapreduce.TaskType, int)"
			)]
		public TaskID(string jtIdentifier, int jobId, bool isMap, int id)
			: this(jtIdentifier, jobId, isMap ? TaskType.Map : TaskType.Reduce, id)
		{
		}

		/// <summary>
		/// Constructs a TaskID object from given
		/// <see cref="JobID"/>
		/// .
		/// </summary>
		/// <param name="jobId">JobID that this tip belongs to</param>
		/// <param name="type">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.TaskType"/>
		/// 
		/// </param>
		/// <param name="id">the tip number</param>
		public TaskID(JobID jobId, TaskType type, int id)
			: base(jobId, type, id)
		{
		}

		/// <summary>Constructs a TaskInProgressId object from given parts.</summary>
		/// <param name="jtIdentifier">jobTracker identifier</param>
		/// <param name="jobId">job number</param>
		/// <param name="type">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.TaskType"/>
		/// 
		/// </param>
		/// <param name="id">the tip number</param>
		public TaskID(string jtIdentifier, int jobId, TaskType type, int id)
			: this(new JobID(jtIdentifier, jobId), type, id)
		{
		}

		public TaskID()
			: base(new JobID(), TaskType.Reduce, 0)
		{
		}

		/// <summary>Downgrade a new TaskID to an old one</summary>
		/// <param name="old">a new or old TaskID</param>
		/// <returns>either old or a new TaskID build to match old</returns>
		public static Org.Apache.Hadoop.Mapred.TaskID Downgrade(Org.Apache.Hadoop.Mapreduce.TaskID
			 old)
		{
			if (old is Org.Apache.Hadoop.Mapred.TaskID)
			{
				return (Org.Apache.Hadoop.Mapred.TaskID)old;
			}
			else
			{
				return new Org.Apache.Hadoop.Mapred.TaskID(JobID.Downgrade(old.GetJobID()), old.GetTaskType
					(), old.GetId());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public static Org.Apache.Hadoop.Mapred.TaskID Read(DataInput @in)
		{
			Org.Apache.Hadoop.Mapred.TaskID tipId = new Org.Apache.Hadoop.Mapred.TaskID();
			tipId.ReadFields(@in);
			return tipId;
		}

		public override JobID GetJobID()
		{
			return (JobID)base.GetJobID();
		}

		/// <summary>Returns a regex pattern which matches task IDs.</summary>
		/// <remarks>
		/// Returns a regex pattern which matches task IDs. Arguments can
		/// be given null, in which case that part of the regex will be generic.
		/// For example to obtain a regex matching <i>the first map task</i>
		/// of <i>any jobtracker</i>, of <i>any job</i>, we would use :
		/// <pre>
		/// TaskID.getTaskIDsPattern(null, null, true, 1);
		/// </pre>
		/// which will return :
		/// <pre> "task_[^_]*_[0-9]*_m_000001*" </pre>
		/// </remarks>
		/// <param name="jtIdentifier">jobTracker identifier, or null</param>
		/// <param name="jobId">job number, or null</param>
		/// <param name="isMap">whether the tip is a map, or null</param>
		/// <param name="taskId">taskId number, or null</param>
		/// <returns>a regex pattern matching TaskIDs</returns>
		[System.ObsoleteAttribute(@"Use GetTaskIDsPattern(string, int, Org.Apache.Hadoop.Mapreduce.TaskType, int)"
			)]
		public static string GetTaskIDsPattern(string jtIdentifier, int jobId, bool isMap
			, int taskId)
		{
			return GetTaskIDsPattern(jtIdentifier, jobId, isMap ? TaskType.Map : TaskType.Reduce
				, taskId);
		}

		/// <summary>Returns a regex pattern which matches task IDs.</summary>
		/// <remarks>
		/// Returns a regex pattern which matches task IDs. Arguments can
		/// be given null, in which case that part of the regex will be generic.
		/// For example to obtain a regex matching <i>the first map task</i>
		/// of <i>any jobtracker</i>, of <i>any job</i>, we would use :
		/// <pre>
		/// TaskID.getTaskIDsPattern(null, null, true, 1);
		/// </pre>
		/// which will return :
		/// <pre> "task_[^_]*_[0-9]*_m_000001*" </pre>
		/// </remarks>
		/// <param name="jtIdentifier">jobTracker identifier, or null</param>
		/// <param name="jobId">job number, or null</param>
		/// <param name="type">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.TaskType"/>
		/// , or null
		/// </param>
		/// <param name="taskId">taskId number, or null</param>
		/// <returns>a regex pattern matching TaskIDs</returns>
		[Obsolete]
		public static string GetTaskIDsPattern(string jtIdentifier, int jobId, TaskType type
			, int taskId)
		{
			StringBuilder builder = new StringBuilder(Task).Append(Separator).Append(GetTaskIDsPatternWOPrefix
				(jtIdentifier, jobId, type, taskId));
			return builder.ToString();
		}

		[Obsolete]
		internal static StringBuilder GetTaskIDsPatternWOPrefix(string jtIdentifier, int 
			jobId, TaskType type, int taskId)
		{
			StringBuilder builder = new StringBuilder();
			builder.Append(JobID.GetJobIDsPatternWOPrefix(jtIdentifier, jobId)).Append(Separator
				).Append(type != null ? (Org.Apache.Hadoop.Mapreduce.TaskID.GetRepresentingCharacter
				(type)) : Org.Apache.Hadoop.Mapreduce.TaskID.GetAllTaskTypes()).Append(Separator
				).Append(taskId != null ? idFormat.Format(taskId) : "[0-9]*");
			return builder;
		}

		/// <exception cref="System.ArgumentException"/>
		public static Org.Apache.Hadoop.Mapreduce.TaskID ForName(string str)
		{
			return (Org.Apache.Hadoop.Mapred.TaskID)Org.Apache.Hadoop.Mapreduce.TaskID.ForName
				(str);
		}
	}
}
