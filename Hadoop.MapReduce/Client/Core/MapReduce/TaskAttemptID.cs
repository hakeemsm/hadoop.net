using System;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
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
	public class TaskAttemptID : ID
	{
		protected internal const string Attempt = "attempt";

		private TaskID taskId;

		/// <summary>
		/// Constructs a TaskAttemptID object from given
		/// <see cref="TaskID"/>
		/// .
		/// </summary>
		/// <param name="taskId">TaskID that this task belongs to</param>
		/// <param name="id">the task attempt number</param>
		public TaskAttemptID(TaskID taskId, int id)
			: base(id)
		{
			if (taskId == null)
			{
				throw new ArgumentException("taskId cannot be null");
			}
			this.taskId = taskId;
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

		/// <summary>Constructs a TaskId object from given parts.</summary>
		/// <param name="jtIdentifier">jobTracker identifier</param>
		/// <param name="jobId">job number</param>
		/// <param name="isMap">whether the tip is a map</param>
		/// <param name="taskId">taskId number</param>
		/// <param name="id">the task attempt number</param>
		[Obsolete]
		public TaskAttemptID(string jtIdentifier, int jobId, bool isMap, int taskId, int 
			id)
			: this(new TaskID(jtIdentifier, jobId, isMap, taskId), id)
		{
		}

		public TaskAttemptID()
		{
			taskId = new TaskID();
		}

		/// <summary>
		/// Returns the
		/// <see cref="JobID"/>
		/// object that this task attempt belongs to
		/// </summary>
		public virtual JobID GetJobID()
		{
			return taskId.GetJobID();
		}

		/// <summary>
		/// Returns the
		/// <see cref="TaskID"/>
		/// object that this task attempt belongs to
		/// </summary>
		public virtual TaskID GetTaskID()
		{
			return taskId;
		}

		/// <summary>Returns whether this TaskID is a map ID</summary>
		[Obsolete]
		public virtual bool IsMap()
		{
			return taskId.IsMap();
		}

		/// <summary>Returns the TaskType of the TaskAttemptID</summary>
		public virtual TaskType GetTaskType()
		{
			return taskId.GetTaskType();
		}

		public override bool Equals(object o)
		{
			if (!base.Equals(o))
			{
				return false;
			}
			Org.Apache.Hadoop.Mapreduce.TaskAttemptID that = (Org.Apache.Hadoop.Mapreduce.TaskAttemptID
				)o;
			return this.taskId.Equals(that.taskId);
		}

		/// <summary>Add the unique string to the StringBuilder</summary>
		/// <param name="builder">the builder to append ot</param>
		/// <returns>the builder that was passed in.</returns>
		protected internal virtual StringBuilder AppendTo(StringBuilder builder)
		{
			return taskId.AppendTo(builder).Append(Separator).Append(id);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			base.ReadFields(@in);
			taskId.ReadFields(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			base.Write(@out);
			taskId.Write(@out);
		}

		public override int GetHashCode()
		{
			return taskId.GetHashCode() * 5 + id;
		}

		/// <summary>Compare TaskIds by first tipIds, then by task numbers.</summary>
		public override int CompareTo(ID o)
		{
			Org.Apache.Hadoop.Mapreduce.TaskAttemptID that = (Org.Apache.Hadoop.Mapreduce.TaskAttemptID
				)o;
			int tipComp = this.taskId.CompareTo(that.taskId);
			if (tipComp == 0)
			{
				return this.id - that.id;
			}
			else
			{
				return tipComp;
			}
		}

		public override string ToString()
		{
			return AppendTo(new StringBuilder(Attempt)).ToString();
		}

		/// <summary>Construct a TaskAttemptID object from given string</summary>
		/// <returns>constructed TaskAttemptID object or null if the given String is null</returns>
		/// <exception cref="System.ArgumentException">if the given string is malformed</exception>
		public static Org.Apache.Hadoop.Mapreduce.TaskAttemptID ForName(string str)
		{
			if (str == null)
			{
				return null;
			}
			string exceptionMsg = null;
			try
			{
				string[] parts = str.Split(char.ToString(Separator));
				if (parts.Length == 6)
				{
					if (parts[0].Equals(Attempt))
					{
						string type = parts[3];
						TaskType t = TaskID.GetTaskType(type[0]);
						if (t != null)
						{
							return new Org.Apache.Hadoop.Mapred.TaskAttemptID(parts[1], System.Convert.ToInt32
								(parts[2]), t, System.Convert.ToInt32(parts[4]), System.Convert.ToInt32(parts[5]
								));
						}
						else
						{
							exceptionMsg = "Bad TaskType identifier. TaskAttemptId string : " + str + " is not properly formed.";
						}
					}
				}
			}
			catch (Exception)
			{
			}
			//fall below
			if (exceptionMsg == null)
			{
				exceptionMsg = "TaskAttemptId string : " + str + " is not properly formed";
			}
			throw new ArgumentException(exceptionMsg);
		}
	}
}
