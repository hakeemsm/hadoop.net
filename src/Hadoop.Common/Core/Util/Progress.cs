using System.Text;
using Org.Apache.Commons.Logging;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>Utility to assist with generation of progress reports.</summary>
	/// <remarks>
	/// Utility to assist with generation of progress reports.  Applications build
	/// a hierarchy of
	/// <see cref="Progress"/>
	/// instances, each modelling a phase of
	/// execution.  The root is constructed with
	/// <see cref="Progress()"/>
	/// .  Nodes for
	/// sub-phases are created by calling
	/// <see cref="AddPhase()"/>
	/// .
	/// </remarks>
	public class Progress
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Util.Progress
			));

		private string status = string.Empty;

		private float progress;

		private int currentPhase;

		private AList<Org.Apache.Hadoop.Util.Progress> phases = new AList<Org.Apache.Hadoop.Util.Progress
			>();

		private Org.Apache.Hadoop.Util.Progress parent;

		private bool fixedWeightageForAllPhases = false;

		private float progressPerPhase = 0.0f;

		private AList<float> progressWeightagesForPhases = new AList<float>();

		/// <summary>Creates a new root node.</summary>
		public Progress()
		{
		}

		// Each phase can have different progress weightage. For example, in
		// Map Task, map phase accounts for 66.7% and sort phase for 33.3%.
		// User needs to give weightages as parameters to all phases(when adding
		// phases) in a Progress object, if he wants to give weightage to any of the
		// phases. So when nodes are added without specifying weightage, it means 
		// fixed weightage for all phases.
		/// <summary>Adds a named node to the tree.</summary>
		public virtual Org.Apache.Hadoop.Util.Progress AddPhase(string status)
		{
			Org.Apache.Hadoop.Util.Progress phase = AddPhase();
			phase.SetStatus(status);
			return phase;
		}

		/// <summary>Adds a node to the tree.</summary>
		/// <remarks>Adds a node to the tree. Gives equal weightage to all phases</remarks>
		public virtual Org.Apache.Hadoop.Util.Progress AddPhase()
		{
			lock (this)
			{
				Org.Apache.Hadoop.Util.Progress phase = AddNewPhase();
				// set equal weightage for all phases
				progressPerPhase = 1.0f / phases.Count;
				fixedWeightageForAllPhases = true;
				return phase;
			}
		}

		/// <summary>Adds a new phase.</summary>
		/// <remarks>Adds a new phase. Caller needs to set progress weightage</remarks>
		private Org.Apache.Hadoop.Util.Progress AddNewPhase()
		{
			lock (this)
			{
				Org.Apache.Hadoop.Util.Progress phase = new Org.Apache.Hadoop.Util.Progress();
				phases.AddItem(phase);
				phase.SetParent(this);
				return phase;
			}
		}

		/// <summary>Adds a named node with a specified progress weightage to the tree.</summary>
		public virtual Org.Apache.Hadoop.Util.Progress AddPhase(string status, float weightage
			)
		{
			Org.Apache.Hadoop.Util.Progress phase = AddPhase(weightage);
			phase.SetStatus(status);
			return phase;
		}

		/// <summary>Adds a node with a specified progress weightage to the tree.</summary>
		public virtual Org.Apache.Hadoop.Util.Progress AddPhase(float weightage)
		{
			lock (this)
			{
				Org.Apache.Hadoop.Util.Progress phase = new Org.Apache.Hadoop.Util.Progress();
				progressWeightagesForPhases.AddItem(weightage);
				phases.AddItem(phase);
				phase.SetParent(this);
				// Ensure that the sum of weightages does not cross 1.0
				float sum = 0;
				for (int i = 0; i < phases.Count; i++)
				{
					sum += progressWeightagesForPhases[i];
				}
				if (sum > 1.0)
				{
					Log.Warn("Sum of weightages can not be more than 1.0; But sum = " + sum);
				}
				return phase;
			}
		}

		/// <summary>Adds n nodes to the tree.</summary>
		/// <remarks>Adds n nodes to the tree. Gives equal weightage to all phases</remarks>
		public virtual void AddPhases(int n)
		{
			lock (this)
			{
				for (int i = 0; i < n; i++)
				{
					AddNewPhase();
				}
				// set equal weightage for all phases
				progressPerPhase = 1.0f / phases.Count;
				fixedWeightageForAllPhases = true;
			}
		}

		/// <summary>returns progress weightage of the given phase</summary>
		/// <param name="phaseNum">
		/// the phase number of the phase(child node) for which we need
		/// progress weightage
		/// </param>
		/// <returns>returns the progress weightage of the specified phase</returns>
		internal virtual float GetProgressWeightage(int phaseNum)
		{
			if (fixedWeightageForAllPhases)
			{
				return progressPerPhase;
			}
			// all phases are of equal weightage
			return progressWeightagesForPhases[phaseNum];
		}

		internal virtual Org.Apache.Hadoop.Util.Progress GetParent()
		{
			lock (this)
			{
				return parent;
			}
		}

		internal virtual void SetParent(Org.Apache.Hadoop.Util.Progress parent)
		{
			lock (this)
			{
				this.parent = parent;
			}
		}

		/// <summary>
		/// Called during execution to move to the next phase at this level in the
		/// tree.
		/// </summary>
		public virtual void StartNextPhase()
		{
			lock (this)
			{
				currentPhase++;
			}
		}

		/// <summary>Returns the current sub-node executing.</summary>
		public virtual Org.Apache.Hadoop.Util.Progress Phase()
		{
			lock (this)
			{
				return phases[currentPhase];
			}
		}

		/// <summary>Completes this node, moving the parent node to its next child.</summary>
		public virtual void Complete()
		{
			// we have to traverse up to our parent, so be careful about locking.
			Org.Apache.Hadoop.Util.Progress myParent;
			lock (this)
			{
				progress = 1.0f;
				myParent = parent;
			}
			if (myParent != null)
			{
				// this will synchronize on the parent, so we make sure we release
				// our lock before getting the parent's, since we're traversing 
				// against the normal traversal direction used by get() or toString().
				// We don't need transactional semantics, so we're OK doing this. 
				myParent.StartNextPhase();
			}
		}

		/// <summary>Called during execution on a leaf node to set its progress.</summary>
		public virtual void Set(float progress)
		{
			lock (this)
			{
				if (float.IsNaN(progress))
				{
					progress = 0;
					Log.Debug("Illegal progress value found, progress is Float.NaN. " + "Progress will be changed to 0"
						);
				}
				else
				{
					if (progress == float.NegativeInfinity)
					{
						progress = 0;
						Log.Debug("Illegal progress value found, progress is " + "Float.NEGATIVE_INFINITY. Progress will be changed to 0"
							);
					}
					else
					{
						if (progress < 0)
						{
							progress = 0;
							Log.Debug("Illegal progress value found, progress is less than 0." + " Progress will be changed to 0"
								);
						}
						else
						{
							if (progress > 1)
							{
								progress = 1;
								Log.Debug("Illegal progress value found, progress is larger than 1." + " Progress will be changed to 1"
									);
							}
							else
							{
								if (progress == float.PositiveInfinity)
								{
									progress = 1;
									Log.Debug("Illegal progress value found, progress is " + "Float.POSITIVE_INFINITY. Progress will be changed to 1"
										);
								}
							}
						}
					}
				}
				this.progress = progress;
			}
		}

		/// <summary>Returns the overall progress of the root.</summary>
		public virtual float Get()
		{
			lock (this)
			{
				// this method probably does not need to be synchronized as getInternal() is
				// synchronized and the node's parent never changes. Still, it doesn't hurt. 
				Org.Apache.Hadoop.Util.Progress node = this;
				while (node.GetParent() != null)
				{
					// find the root
					node = parent;
				}
				return node.GetInternal();
			}
		}

		/// <summary>Returns progress in this node.</summary>
		/// <remarks>
		/// Returns progress in this node. get() would give overall progress of the
		/// root node(not just given current node).
		/// </remarks>
		public virtual float GetProgress()
		{
			lock (this)
			{
				return GetInternal();
			}
		}

		/// <summary>Computes progress in this node.</summary>
		private float GetInternal()
		{
			lock (this)
			{
				int phaseCount = phases.Count;
				if (phaseCount != 0)
				{
					float subProgress = 0.0f;
					float progressFromCurrentPhase = 0.0f;
					if (currentPhase < phaseCount)
					{
						subProgress = Phase().GetInternal();
						progressFromCurrentPhase = GetProgressWeightage(currentPhase) * subProgress;
					}
					float progressFromCompletedPhases = 0.0f;
					if (fixedWeightageForAllPhases)
					{
						// same progress weightage for each phase
						progressFromCompletedPhases = progressPerPhase * currentPhase;
					}
					else
					{
						for (int i = 0; i < currentPhase; i++)
						{
							// progress weightages of phases could be different. Add them
							progressFromCompletedPhases += GetProgressWeightage(i);
						}
					}
					return progressFromCompletedPhases + progressFromCurrentPhase;
				}
				else
				{
					return progress;
				}
			}
		}

		public virtual void SetStatus(string status)
		{
			lock (this)
			{
				this.status = status;
			}
		}

		public override string ToString()
		{
			StringBuilder result = new StringBuilder();
			ToString(result);
			return result.ToString();
		}

		private void ToString(StringBuilder buffer)
		{
			lock (this)
			{
				buffer.Append(status);
				if (phases.Count != 0 && currentPhase < phases.Count)
				{
					buffer.Append(" > ");
					Phase().ToString(buffer);
				}
			}
		}
	}
}
