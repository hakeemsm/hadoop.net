using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
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
	public class TaskID : ID
	{
		protected internal const string Task = "task";

		protected internal static readonly NumberFormat idFormat = NumberFormat.GetInstance
			();

		static TaskID()
		{
			idFormat.SetGroupingUsed(false);
			idFormat.SetMinimumIntegerDigits(6);
		}

		private JobID jobId;

		private TaskType type;

		/// <summary>
		/// Constructs a TaskID object from given
		/// <see cref="JobID"/>
		/// .
		/// </summary>
		/// <param name="jobId">JobID that this tip belongs to</param>
		/// <param name="type">
		/// the
		/// <see cref="TaskType"/>
		/// of the task
		/// </param>
		/// <param name="id">the tip number</param>
		public TaskID(JobID jobId, TaskType type, int id)
			: base(id)
		{
			if (jobId == null)
			{
				throw new ArgumentException("jobId cannot be null");
			}
			this.jobId = jobId;
			this.type = type;
		}

		/// <summary>Constructs a TaskInProgressId object from given parts.</summary>
		/// <param name="jtIdentifier">jobTracker identifier</param>
		/// <param name="jobId">job number</param>
		/// <param name="type">the TaskType</param>
		/// <param name="id">the tip number</param>
		public TaskID(string jtIdentifier, int jobId, TaskType type, int id)
			: this(new JobID(jtIdentifier, jobId), type, id)
		{
		}

		/// <summary>
		/// Constructs a TaskID object from given
		/// <see cref="JobID"/>
		/// .
		/// </summary>
		/// <param name="jobId">JobID that this tip belongs to</param>
		/// <param name="isMap">whether the tip is a map</param>
		/// <param name="id">the tip number</param>
		[Obsolete]
		public TaskID(JobID jobId, bool isMap, int id)
			: this(jobId, isMap ? TaskType.Map : TaskType.Reduce, id)
		{
		}

		/// <summary>Constructs a TaskInProgressId object from given parts.</summary>
		/// <param name="jtIdentifier">jobTracker identifier</param>
		/// <param name="jobId">job number</param>
		/// <param name="isMap">whether the tip is a map</param>
		/// <param name="id">the tip number</param>
		[Obsolete]
		public TaskID(string jtIdentifier, int jobId, bool isMap, int id)
			: this(new JobID(jtIdentifier, jobId), isMap, id)
		{
		}

		public TaskID()
		{
			jobId = new JobID();
		}

		/// <summary>
		/// Returns the
		/// <see cref="JobID"/>
		/// object that this tip belongs to
		/// </summary>
		public virtual JobID GetJobID()
		{
			return jobId;
		}

		/// <summary>Returns whether this TaskID is a map ID</summary>
		[Obsolete]
		public virtual bool IsMap()
		{
			return type == TaskType.Map;
		}

		/// <summary>Get the type of the task</summary>
		public virtual TaskType GetTaskType()
		{
			return type;
		}

		public override bool Equals(object o)
		{
			if (!base.Equals(o))
			{
				return false;
			}
			Org.Apache.Hadoop.Mapreduce.TaskID that = (Org.Apache.Hadoop.Mapreduce.TaskID)o;
			return this.type == that.type && this.jobId.Equals(that.jobId);
		}

		/// <summary>Compare TaskInProgressIds by first jobIds, then by tip numbers.</summary>
		/// <remarks>
		/// Compare TaskInProgressIds by first jobIds, then by tip numbers. Reduces are
		/// defined as greater then maps.
		/// </remarks>
		public override int CompareTo(ID o)
		{
			Org.Apache.Hadoop.Mapreduce.TaskID that = (Org.Apache.Hadoop.Mapreduce.TaskID)o;
			int jobComp = this.jobId.CompareTo(that.jobId);
			if (jobComp == 0)
			{
				if (this.type == that.type)
				{
					return this.id - that.id;
				}
				else
				{
					return this.type.CompareTo(that.type);
				}
			}
			else
			{
				return jobComp;
			}
		}

		public override string ToString()
		{
			return AppendTo(new StringBuilder(Task)).ToString();
		}

		/// <summary>Add the unique string to the given builder.</summary>
		/// <param name="builder">the builder to append to</param>
		/// <returns>the builder that was passed in</returns>
		protected internal virtual StringBuilder AppendTo(StringBuilder builder)
		{
			return jobId.AppendTo(builder).Append(Separator).Append(TaskID.CharTaskTypeMaps.GetRepresentingCharacter
				(type)).Append(Separator).Append(idFormat.Format(id));
		}

		public override int GetHashCode()
		{
			return jobId.GetHashCode() * 524287 + id;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			base.ReadFields(@in);
			jobId.ReadFields(@in);
			type = WritableUtils.ReadEnum<TaskType>(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			base.Write(@out);
			jobId.Write(@out);
			WritableUtils.WriteEnum(@out, type);
		}

		/// <summary>Construct a TaskID object from given string</summary>
		/// <returns>constructed TaskID object or null if the given String is null</returns>
		/// <exception cref="System.ArgumentException">if the given string is malformed</exception>
		public static Org.Apache.Hadoop.Mapreduce.TaskID ForName(string str)
		{
			if (str == null)
			{
				return null;
			}
			string exceptionMsg = null;
			try
			{
				string[] parts = str.Split("_");
				if (parts.Length == 5)
				{
					if (parts[0].Equals(Task))
					{
						string type = parts[3];
						TaskType t = TaskID.CharTaskTypeMaps.GetTaskType(type[0]);
						if (t != null)
						{
							return new Org.Apache.Hadoop.Mapred.TaskID(parts[1], System.Convert.ToInt32(parts
								[2]), t, System.Convert.ToInt32(parts[4]));
						}
						else
						{
							exceptionMsg = "Bad TaskType identifier. TaskId string : " + str + " is not properly formed.";
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
				exceptionMsg = "TaskId string : " + str + " is not properly formed";
			}
			throw new ArgumentException(exceptionMsg);
		}

		/// <summary>
		/// Gets the character representing the
		/// <see cref="TaskType"/>
		/// </summary>
		/// <param name="type">the TaskType</param>
		/// <returns>the character</returns>
		public static char GetRepresentingCharacter(TaskType type)
		{
			return TaskID.CharTaskTypeMaps.GetRepresentingCharacter(type);
		}

		/// <summary>
		/// Gets the
		/// <see cref="TaskType"/>
		/// corresponding to the character
		/// </summary>
		/// <param name="c">the character</param>
		/// <returns>the TaskType</returns>
		public static TaskType GetTaskType(char c)
		{
			return TaskID.CharTaskTypeMaps.GetTaskType(c);
		}

		public static string GetAllTaskTypes()
		{
			return TaskID.CharTaskTypeMaps.allTaskTypes;
		}

		/// <summary>
		/// Maintains the mapping from the character representation of a task type to
		/// the enum class TaskType constants
		/// </summary>
		internal class CharTaskTypeMaps
		{
			private static EnumMap<TaskType, char> typeToCharMap = new EnumMap<TaskType, char
				>(typeof(TaskType));

			private static IDictionary<char, TaskType> charToTypeMap = new Dictionary<char, TaskType
				>();

			internal static string allTaskTypes = "(m|r|s|c|t)";

			static CharTaskTypeMaps()
			{
				SetupTaskTypeToCharMapping();
				SetupCharToTaskTypeMapping();
			}

			private static void SetupTaskTypeToCharMapping()
			{
				typeToCharMap[TaskType.Map] = 'm';
				typeToCharMap[TaskType.Reduce] = 'r';
				typeToCharMap[TaskType.JobSetup] = 's';
				typeToCharMap[TaskType.JobCleanup] = 'c';
				typeToCharMap[TaskType.TaskCleanup] = 't';
			}

			private static void SetupCharToTaskTypeMapping()
			{
				charToTypeMap['m'] = TaskType.Map;
				charToTypeMap['r'] = TaskType.Reduce;
				charToTypeMap['s'] = TaskType.JobSetup;
				charToTypeMap['c'] = TaskType.JobCleanup;
				charToTypeMap['t'] = TaskType.TaskCleanup;
			}

			internal static char GetRepresentingCharacter(TaskType type)
			{
				return typeToCharMap[type];
			}

			internal static TaskType GetTaskType(char c)
			{
				return charToTypeMap[c];
			}
		}
	}
}
