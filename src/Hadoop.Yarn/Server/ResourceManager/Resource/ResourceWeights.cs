using System.Text;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource
{
	public class ResourceWeights
	{
		public static readonly Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource.ResourceWeights
			 Neutral = new Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource.ResourceWeights
			(1.0f);

		private float[] weights = new float[ResourceType.Values().Length];

		public ResourceWeights(float memoryWeight, float cpuWeight)
		{
			weights[(int)(ResourceType.Memory)] = memoryWeight;
			weights[(int)(ResourceType.Cpu)] = cpuWeight;
		}

		public ResourceWeights(float weight)
		{
			SetWeight(weight);
		}

		public ResourceWeights()
		{
		}

		public virtual void SetWeight(float weight)
		{
			for (int i = 0; i < weights.Length; i++)
			{
				weights[i] = weight;
			}
		}

		public virtual void SetWeight(ResourceType resourceType, float weight)
		{
			weights[(int)(resourceType)] = weight;
		}

		public virtual float GetWeight(ResourceType resourceType)
		{
			return weights[(int)(resourceType)];
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("<");
			for (int i = 0; i < ResourceType.Values().Length; i++)
			{
				if (i != 0)
				{
					sb.Append(", ");
				}
				ResourceType resourceType = ResourceType.Values()[i];
				sb.Append(StringUtils.ToLowerCase(resourceType.ToString()));
				sb.Append(string.Format(" weight=%.1f", GetWeight(resourceType)));
			}
			sb.Append(">");
			return sb.ToString();
		}
	}
}
