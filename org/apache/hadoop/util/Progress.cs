using Sharpen;

namespace org.apache.hadoop.util
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
	/// <see cref="addPhase()"/>
	/// .
	/// </remarks>
	public class Progress
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.Progress))
			);

		private string status = string.Empty;

		private float progress;

		private int currentPhase;

		private System.Collections.Generic.List<org.apache.hadoop.util.Progress> phases = 
			new System.Collections.Generic.List<org.apache.hadoop.util.Progress>();

		private org.apache.hadoop.util.Progress parent;

		private bool fixedWeightageForAllPhases = false;

		private float progressPerPhase = 0.0f;

		private System.Collections.Generic.List<float> progressWeightagesForPhases = new 
			System.Collections.Generic.List<float>();

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
		public virtual org.apache.hadoop.util.Progress addPhase(string status)
		{
			org.apache.hadoop.util.Progress phase = addPhase();
			phase.setStatus(status);
			return phase;
		}

		/// <summary>Adds a node to the tree.</summary>
		/// <remarks>Adds a node to the tree. Gives equal weightage to all phases</remarks>
		public virtual org.apache.hadoop.util.Progress addPhase()
		{
			lock (this)
			{
				org.apache.hadoop.util.Progress phase = addNewPhase();
				// set equal weightage for all phases
				progressPerPhase = 1.0f / phases.Count;
				fixedWeightageForAllPhases = true;
				return phase;
			}
		}

		/// <summary>Adds a new phase.</summary>
		/// <remarks>Adds a new phase. Caller needs to set progress weightage</remarks>
		private org.apache.hadoop.util.Progress addNewPhase()
		{
			lock (this)
			{
				org.apache.hadoop.util.Progress phase = new org.apache.hadoop.util.Progress();
				phases.add(phase);
				phase.setParent(this);
				return phase;
			}
		}

		/// <summary>Adds a named node with a specified progress weightage to the tree.</summary>
		public virtual org.apache.hadoop.util.Progress addPhase(string status, float weightage
			)
		{
			org.apache.hadoop.util.Progress phase = addPhase(weightage);
			phase.setStatus(status);
			return phase;
		}

		/// <summary>Adds a node with a specified progress weightage to the tree.</summary>
		public virtual org.apache.hadoop.util.Progress addPhase(float weightage)
		{
			lock (this)
			{
				org.apache.hadoop.util.Progress phase = new org.apache.hadoop.util.Progress();
				progressWeightagesForPhases.add(weightage);
				phases.add(phase);
				phase.setParent(this);
				// Ensure that the sum of weightages does not cross 1.0
				float sum = 0;
				for (int i = 0; i < phases.Count; i++)
				{
					sum += progressWeightagesForPhases[i];
				}
				if (sum > 1.0)
				{
					LOG.warn("Sum of weightages can not be more than 1.0; But sum = " + sum);
				}
				return phase;
			}
		}

		/// <summary>Adds n nodes to the tree.</summary>
		/// <remarks>Adds n nodes to the tree. Gives equal weightage to all phases</remarks>
		public virtual void addPhases(int n)
		{
			lock (this)
			{
				for (int i = 0; i < n; i++)
				{
					addNewPhase();
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
		internal virtual float getProgressWeightage(int phaseNum)
		{
			if (fixedWeightageForAllPhases)
			{
				return progressPerPhase;
			}
			// all phases are of equal weightage
			return progressWeightagesForPhases[phaseNum];
		}

		internal virtual org.apache.hadoop.util.Progress getParent()
		{
			lock (this)
			{
				return parent;
			}
		}

		internal virtual void setParent(org.apache.hadoop.util.Progress parent)
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
		public virtual void startNextPhase()
		{
			lock (this)
			{
				currentPhase++;
			}
		}

		/// <summary>Returns the current sub-node executing.</summary>
		public virtual org.apache.hadoop.util.Progress phase()
		{
			lock (this)
			{
				return phases[currentPhase];
			}
		}

		/// <summary>Completes this node, moving the parent node to its next child.</summary>
		public virtual void complete()
		{
			// we have to traverse up to our parent, so be careful about locking.
			org.apache.hadoop.util.Progress myParent;
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
				myParent.startNextPhase();
			}
		}

		/// <summary>Called during execution on a leaf node to set its progress.</summary>
		public virtual void set(float progress)
		{
			lock (this)
			{
				if (float.IsNaN(progress))
				{
					progress = 0;
					LOG.debug("Illegal progress value found, progress is Float.NaN. " + "Progress will be changed to 0"
						);
				}
				else
				{
					if (progress == float.NegativeInfinity)
					{
						progress = 0;
						LOG.debug("Illegal progress value found, progress is " + "Float.NEGATIVE_INFINITY. Progress will be changed to 0"
							);
					}
					else
					{
						if (progress < 0)
						{
							progress = 0;
							LOG.debug("Illegal progress value found, progress is less than 0." + " Progress will be changed to 0"
								);
						}
						else
						{
							if (progress > 1)
							{
								progress = 1;
								LOG.debug("Illegal progress value found, progress is larger than 1." + " Progress will be changed to 1"
									);
							}
							else
							{
								if (progress == float.PositiveInfinity)
								{
									progress = 1;
									LOG.debug("Illegal progress value found, progress is " + "Float.POSITIVE_INFINITY. Progress will be changed to 1"
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
		public virtual float get()
		{
			lock (this)
			{
				// this method probably does not need to be synchronized as getInternal() is
				// synchronized and the node's parent never changes. Still, it doesn't hurt. 
				org.apache.hadoop.util.Progress node = this;
				while (node.getParent() != null)
				{
					// find the root
					node = parent;
				}
				return node.getInternal();
			}
		}

		/// <summary>Returns progress in this node.</summary>
		/// <remarks>
		/// Returns progress in this node. get() would give overall progress of the
		/// root node(not just given current node).
		/// </remarks>
		public virtual float getProgress()
		{
			lock (this)
			{
				return getInternal();
			}
		}

		/// <summary>Computes progress in this node.</summary>
		private float getInternal()
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
						subProgress = phase().getInternal();
						progressFromCurrentPhase = getProgressWeightage(currentPhase) * subProgress;
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
							progressFromCompletedPhases += getProgressWeightage(i);
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

		public virtual void setStatus(string status)
		{
			lock (this)
			{
				this.status = status;
			}
		}

		public override string ToString()
		{
			java.lang.StringBuilder result = new java.lang.StringBuilder();
			toString(result);
			return result.ToString();
		}

		private void toString(java.lang.StringBuilder buffer)
		{
			lock (this)
			{
				buffer.Append(status);
				if (phases.Count != 0 && currentPhase < phases.Count)
				{
					buffer.Append(" > ");
					phase().toString(buffer);
				}
			}
		}
	}
}
