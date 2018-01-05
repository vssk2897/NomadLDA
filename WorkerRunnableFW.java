import cc.mallet.topics.TopicAssignment;
import java.util.Arrays;
import java.util.ArrayList;

import java.util.zip.*;

import java.io.*;
import java.text.NumberFormat;

import cc.mallet.types.*;
import cc.mallet.util.Randoms;

/**
 * A parallel topic model runnable task.
 * 
 * @author Nagesh B S, V S S Karthik
 */ 

public class WorkerRunnableFW implements Runnable {
	
	boolean isFinished = true;

	ArrayList<TopicAssignment> data;
	int startDoc, numDocs;

	protected int numTopics; // Number of topics to be fit

	// These values are used to encode type/topic counts as
	//  count/topic pairs in a single int.
        
        
	protected int numTypes;

	protected double[] alpha;	 // Dirichlet(alpha,alpha,...) is the distribution over topics
	protected double alphaSum;
	protected double beta;   // Prior on per-topic multinomial distribution over words
	protected double betaSum;
	public static final double DEFAULT_BETA = 0.01;
        public  double[] fenwick;
        public int nonZeroIndex;
	protected double smoothingOnlyMass = 0.0;
	protected double[] cachedCoefficients;
        public int bin=0,fen=0;
	protected int[][] wordTopicCounts;
        protected int[][] wordTopicIndex;
	protected int[] tokensPerTopic; // indexed by <topic index>

	// for dirichlet estimation
	protected int[] docLengthCounts; // histogram of document sizes
	protected int[][] topicDocCounts; // histogram of document/topic counts, indexed by <topic index, sequence position index>

	boolean shouldSaveState = false;
	boolean shouldBuildLocalCounts = true;
	
	protected Randoms random;
	
	public WorkerRunnableFW (int numTopics,
						   double[] alpha, double alphaSum,
						   double beta, Randoms random,
						   ArrayList<TopicAssignment> data,
						   int[][] wordTopicCounts, int[][] typeTopicIndex,
                                                   int[] tokensPerTopic,
						   int startDoc, int numDocs) {

		this.data = data;

		this.numTopics = numTopics;
		this.numTypes = wordTopicCounts.length;
                wordTopicIndex=typeTopicIndex;
                

		this.wordTopicCounts = wordTopicCounts;
		this.tokensPerTopic = tokensPerTopic;
		
                
                
                
		this.alphaSum = alphaSum;
		this.alpha = alpha;
		this.beta = beta;
		this.betaSum = beta * numTypes;
		this.random = random;
		
		this.startDoc = startDoc;
		this.numDocs = numDocs;

		cachedCoefficients = new double[ numTopics ];

		//System.err.println("WorkerRunnable Thread: " + numTopics + " topics, " + topicBits + " topic bits, " + 
		//				   Integer.toBinaryString(topicMask) + " topic mask");

	}

	/**
	 *  If there is only one thread, we don't need to go through 
	 *   communication overhead. This method asks this worker not
	 *   to prepare local type-topic counts. The method should be
	 *   called when we are using this code in a non-threaded environment.
	 */
	public void makeOnlyThread() {
		shouldBuildLocalCounts = false;
	}

	public int[] getTokensPerTopic() { return tokensPerTopic; }
	public int[][] getWordTopicCounts() { return wordTopicCounts; }
        public int[][] getWordTopicIndex(){return wordTopicIndex; }
	public int[] getDocLengthCounts() { return docLengthCounts; }
	public int[][] getTopicDocCounts() { return topicDocCounts; }

	public void initializeAlphaStatistics(int size) {
		docLengthCounts = new int[size];
		topicDocCounts = new int[numTopics][size];
	}
	
	public void collectAlphaStatistics() {
		shouldSaveState = true;
	}

	public void resetBeta(double beta, double betaSum) {
		this.beta = beta;
		this.betaSum = betaSum;
	}

	/**
	 *  Once we have sampled the local counts, trash the 
	 *   "global" type topic counts and reuse the space to 
	 *   build a summary of the type topic counts specific to 
	 *   this worker's section of the corpus.
	 */
	public void buildLocalWordTopicCounts () {

		// Clear the topic totals
		Arrays.fill(tokensPerTopic, 0);

		// Clear the type/topic counts, only 
		//  looking at the entries before the first 0 entry.
		for (int type = 0; type < wordTopicIndex.length; type++) {

			int[] topicCounts = wordTopicCounts[type];
			
			int position = 0;
			while (position < topicCounts.length && 
				   topicCounts[position] > 0) {
				topicCounts[position] = 0;
				position++;
			}
		}

        for (int doc = startDoc;
			 doc < data.size() && doc < startDoc + numDocs;
             doc++) {

			TopicAssignment document = data.get(doc);

            FeatureSequence tokens = (FeatureSequence) document.instance.getData();
            FeatureSequence topicSequence =  (FeatureSequence) document.topicSequence;

            int[] topics = topicSequence.getFeatures();
            for (int position = 0; position < tokens.size(); position++) {

				int topic = topics[position];

				if (topic == ParallelFWTopicModel.UNASSIGNED_TOPIC) { continue; }

				tokensPerTopic[topic]++;
				
				// The format for these arrays is 
				//  the topic in the rightmost bits
				//  the count in the remaining (left) bits.
				// Since the count is in the high bits, sorting (desc)
				//  by the numeric value of the int guarantees that
				//  higher counts will be before the lower counts.
				
				int type = tokens.getIndexAtPosition(position);

				int[] currentWordTopicCounts = wordTopicCounts[ type ];
				int[] currentWordTopicIndex = wordTopicIndex[type];
				// Start by assuming that the array is either empty
				//  or is in sorted (descending) order.
				
				// Here we are only adding counts, so if we find 
				//  an existing location with the topic, we only need
				//  to ensure that it is not larger than its left neighbor.
				
				int index = 0;
				int currentTopic = currentWordTopicIndex[index] ;
				int currentValue;
				
                                while(index< currentWordTopicIndex.length &&currentWordTopicCounts[currentWordTopicIndex[index]]>0 && currentTopic != topic)
                                {
                                    index++;
                                    if(index == currentWordTopicIndex.length)
                                        System.out.println("overflow on type " + type);
                                    else
                                        currentTopic=currentWordTopicIndex[index];
                                }
                                if(index==currentWordTopicIndex.length)
                                    index-=1;
                                currentValue=currentWordTopicCounts[currentWordTopicIndex[index]];
				if (currentValue == 0) {
					// new value is 1, so we don't have to worry about sorting
					//  (except by topic suffix, which doesn't matter)
					currentWordTopicIndex[index]=topic;
					currentWordTopicCounts[currentWordTopicIndex[index]] = 1;
                                        
				}
				else {
                                        currentWordTopicIndex[index]=topic;
					currentWordTopicCounts[currentWordTopicIndex[index]] = currentValue + 1;
					
					// Now ensure that the array is still sorted by 
					//  bubbling this value up.
					while (index>0&&
                                                currentWordTopicIndex[index] > 0 &&
						   currentWordTopicCounts[currentWordTopicIndex[index]] > 
                                                currentWordTopicCounts[currentWordTopicIndex[index - 1]]) {
						int temp = currentWordTopicIndex[index];
						currentWordTopicIndex[index] = currentWordTopicIndex[index - 1];
						currentWordTopicIndex[index - 1] = temp;
						
						index--;
					}
				}
			}
		}

	}


        
	public void run () {

		try {
			
			if (! isFinished) { System.out.println("already running!"); return; }
			
			isFinished = false;
			
			// Initialize the smoothing-only sampling bucket
			smoothingOnlyMass = 0;
			
			// Initialize the cached coefficients, using only smoothing.
			//  These values will be selectively replaced in documents with
			//  non-zero counts in particular topics.
			
			for (int topic=0; topic < numTopics; topic++) {
				//smoothingOnlyMass += alpha[topic] * beta / (tokensPerTopic[topic] + betaSum);
				cachedCoefficients[topic] =  alpha[topic] / (tokensPerTopic[topic] + betaSum);
			}
			
			for (int doc = startDoc;
				 doc < data.size() && doc < startDoc + numDocs;
				 doc++) {
				
				/*
				  if (doc % 10000 == 0) {
				  System.out.println("processing doc " + doc);
				  }
				*/
				
				FeatureSequence tokenSequence =
					(FeatureSequence) data.get(doc).instance.getData();
				LabelSequence topicSequence =
					(LabelSequence) data.get(doc).topicSequence;
				
				sampleTopicsForOneDoc (tokenSequence, topicSequence,
									   true);
			}
			
			if (shouldBuildLocalCounts) {
				buildLocalWordTopicCounts();
			}

			shouldSaveState = false;
			isFinished = true;
                        //System.out.println("No. of cases for binary Search "+bin+"No. of cases for F + tree "+fen);
		} catch (Exception e) {
			isFinished = true;
                        e.printStackTrace();
		}
	}
	
	protected void sampleTopicsForOneDoc (FeatureSequence tokenSequence,
										  FeatureSequence topicSequence,
										  boolean readjustTopicsAndStats /* currently ignored */) {

		int[] oneDocTopics = topicSequence.getFeatures();
		int[] currentWordTopicCounts;
                int[] currentWordTopicIndex;
		int type, oldTopic, newTopic;
		double topicWeightsSum;
		int docLength = tokenSequence.getLength();

		int[] localTopicCounts = new int[numTopics];
		int[] localTopicIndex = new int[numTopics];

		//		populate topic counts
		for (int position = 0; position < docLength; position++) {
			if (oneDocTopics[position] == ParallelFWTopicModel.UNASSIGNED_TOPIC) { continue; }
			localTopicCounts[oneDocTopics[position]]++;
		}

		// Build an array that densely lists the topics that
		//  have non-zero counts.
                double[] qVec = new double[numTopics];
		int denseIndex = 0;
                // How to change this ??
                
                for (int topic=0;topic<numTopics;topic++)
                {
                    if (localTopicCounts[topic] != 0) {
				localTopicIndex[denseIndex] = topic;
                                qVec[topic]=(alpha[topic] + localTopicCounts[topic]) /(double) (tokensPerTopic[topic] + betaSum);
				denseIndex++;
			}
                    else
                        qVec[topic]=(alpha[topic]) /(double) (tokensPerTopic[topic] + betaSum);
                }
		// Construct fenvick tree
		int nonZeroTopics = denseIndex;
                FenwickPlus fp=new FenwickPlus(numTopics);
                fp.construct(qVec);
		//		Initialize the topic count/beta sampling bucket
		double topicBetaMass = 0.0;

		// Initialize cached coefficients and the topic/beta 
		//  normalizing constant

		for (denseIndex = 0; denseIndex < nonZeroTopics; denseIndex++) {
			int topic = localTopicIndex[denseIndex];
			int n = localTopicCounts[topic];

			//	initialize the normalization constant for the (B * n_{t|d}) term
			//topicBetaMass += beta * n /	(tokensPerTopic[topic] + betaSum);	

			//	update the coefficients for the non-zero topics
			cachedCoefficients[topic] =	(alpha[topic] + n) / (tokensPerTopic[topic] + betaSum);
		}

		double topicTermMass = 0.0;

		double[] topicTermScores = new double[numTopics];
		int[] topicTermIndices;
		int[] topicTermValues;
		int i;
		double score;
                newTopic=-1;
		//	Iterate over the positions (words) in the document 
		for (int position = 0; position < docLength; position++) {
			type = tokenSequence.getIndexAtPosition(position);
			oldTopic = oneDocTopics[position];

			currentWordTopicCounts = wordTopicCounts[type];
                        currentWordTopicIndex = wordTopicIndex[type];
			if (oldTopic != ParallelFWTopicModel.UNASSIGNED_TOPIC) {
				//	Remove this token from all counts. 
				
				// Remove this topic's contribution to the 
				//  normalizing constants
                                        
                                fp.update(oldTopic, beta*(alpha[oldTopic]+localTopicCounts[oldTopic])/(double) (tokensPerTopic[oldTopic] + betaSum)*(tokensPerTopic[oldTopic] + betaSum-1));
				// Decrement the local doc/topic counts
				
				localTopicCounts[oldTopic]--;
				
				// Maintain the dense index, if we are deleting
				//  the old topic
				if (localTopicCounts[oldTopic] == 0) {
					
					// First get to the dense location associated with
					//  the old topic.
					
					denseIndex = 0;
					
					// We know it's in there somewhere, so we don't 
					//  need bounds checking.
					while (localTopicIndex[denseIndex] != oldTopic) {
						denseIndex++;
					}
				
					// shift all remaining dense indices to the left.
					while (denseIndex < nonZeroTopics) {
						if (denseIndex < localTopicIndex.length - 1) {
							localTopicIndex[denseIndex] = 
								localTopicIndex[denseIndex + 1];
						}
						denseIndex++;
					}
					
					nonZeroTopics --;
				}

				// Decrement the global topic count totals
				tokensPerTopic[oldTopic]--;
				assert(tokensPerTopic[oldTopic] >= 0) : "old Topic " + oldTopic + " below 0";
			

				// Add the old topic's contribution back into the
				//  normalizing constants. by updating the fenwick tree
                                        
                                fp.update(oldTopic, -(beta*(alpha[oldTopic]+localTopicCounts[oldTopic]) )/(double) (tokensPerTopic[oldTopic] + betaSum)*(tokensPerTopic[oldTopic] + betaSum+1));        
				// Reset the cached coefficient for this topic
				cachedCoefficients[oldTopic] = 
					(alpha[oldTopic] + localTopicCounts[oldTopic]) /
					(tokensPerTopic[oldTopic] + betaSum);
			}


			// Now go over the type/topic counts, decrementing
			//  where appropriate, and calculating the score
			//  for each topic at the same time.

			int index = 0;
			int currentTopic, currentValue;

			boolean alreadyDecremented = (oldTopic == ParallelFWTopicModel.UNASSIGNED_TOPIC);

			topicTermMass = 0.0;
                        
                        int termIndex=0;
			while(index < currentWordTopicIndex.length && currentWordTopicCounts[currentWordTopicIndex[index]]>0)
                        {
                            currentTopic = currentWordTopicIndex[index];
                            currentValue = currentWordTopicCounts[currentTopic] ;

                            if (! alreadyDecremented && 
					currentTopic == oldTopic) {

					currentValue --;
					if (currentValue == 0) {
						currentWordTopicCounts[currentTopic] = 0;
					}
					else {
                                                currentWordTopicIndex[index]=oldTopic;
						currentWordTopicCounts[oldTopic] = currentValue;
					}
					
					// Shift the reduced value to the right, if necessary.

					int subIndex = index;
                                        
                                        while(subIndex < currentWordTopicIndex.length-1 && currentWordTopicIndex[currentWordTopicIndex[subIndex]]
                                                 < currentWordTopicCounts[currentWordTopicIndex[subIndex+1]])
                                        {
                                            int temp = currentWordTopicIndex[subIndex];
						currentWordTopicIndex[subIndex] = currentWordTopicIndex[subIndex + 1];
						currentWordTopicIndex[subIndex + 1] = temp;
						subIndex++;
                                        }

					alreadyDecremented = true;
				}
				else {
					score = 
						cachedCoefficients[currentTopic] * currentValue;
					topicTermMass += score;
                                        //Calculating Cumulative sum
					topicTermScores[index] = topicTermMass;
                                        termIndex++;
					index++;
				}
                        }    
			double sample = random.nextUniform();// * ( fenwick[1] + topicTermMass);
			double origSample = sample;
                        
                        
                        
			//	Make sure it actually gets set
			newTopic = -1;
                        
                        if (sample < topicTermMass) {
				//topicTermCount++;
                                // Use Binary Search for sampling
                                bin++;
				i = 0;
                                while (sample > topicTermScores[i]) {
                                    sample-=topicTermScores[i];
                                    i++;
                                    }
                                
                                
				newTopic = currentWordTopicIndex[i] ;
				currentValue = currentWordTopicCounts[newTopic] ;
				
                                currentWordTopicIndex[i]=newTopic;
				currentWordTopicCounts[currentWordTopicIndex[i]] = currentValue + 1;

				// Bubble the new value up, if necessary
				
				while (i > 0 &&
					   currentWordTopicCounts[currentWordTopicIndex[i]] > currentWordTopicCounts[currentWordTopicIndex[i - 1]]) {
					int temp = currentWordTopicIndex[i];
					currentWordTopicIndex[i] = currentWordTopicIndex[i - 1];
					currentWordTopicIndex[i - 1] = temp;
					i--;
				}

			}
			else {
                                fen++;
                                // Use Fenwick+ Tree Sampling
				sample -= topicTermMass;
                                // FenwickPlus Tree Sampling to be applied here
				//betaTopicCount++;
                                sample /= beta;
                                newTopic=fp.sample(sample);
                                newTopic-=nonZeroIndex;
                                if( newTopic>numTopics || newTopic< 0 )
                                    System.out.println("Error occurred while Sampling by Fenwick Tree "+newTopic +" "+numTopics);
                        }  
                                
				// Move to the position for the new topic,
				//  which may be the first empty position if this
				//  is a new topic for this word.
				index = 0;
                                
                                //Going to apply Binary Search
                                QuickSort qt = new QuickSort();
                                index=qt.binarySearch(currentWordTopicCounts, currentWordTopicIndex.length, currentWordTopicIndex, newTopic);
                                if(index<0)
                                {
                                    index=0;
                                    while(index<currentWordTopicIndex.length && currentWordTopicCounts[index]!=0)
                                        index++;
                                }            
				// index should now be set to the position of the new topic,
				//  which may be an empty cell at the end of the list.
                                if(index == currentWordTopicIndex.length)
                                    index=0;
				if (currentWordTopicCounts[currentWordTopicIndex[index]] == 0) {
					// inserting a new topic, guaranteed to be in
					//  order w.r.t. count, if not topic.
                                        currentWordTopicIndex[index]=newTopic;
					currentWordTopicCounts[currentWordTopicIndex[index]] = 1 ;
				}
				else {
					currentValue = currentWordTopicCounts[currentWordTopicIndex[index]] ;
					currentWordTopicIndex[index]=newTopic;
                                        currentWordTopicCounts[currentWordTopicIndex[index]] = currentValue + 1;
                                        

					// Bubble the increased value left, if necessary
					while (index > 0 &&
						   currentWordTopicCounts[currentWordTopicIndex[index]] > currentWordTopicCounts[currentWordTopicIndex[index - 1]]) {
						int temp = currentWordTopicIndex[index];
						currentWordTopicIndex[index] = currentWordTopicIndex[index - 1];
						currentWordTopicIndex[index - 1] = temp;

						index--;
					}
				}

			
                        if (newTopic == -1) {
                                                
				newTopic = numTopics-1; // TODO is this appropriate
				//throw new IllegalStateException ("WorkerRunnable: New topic not sampled.");
			}
			//assert(newTopic != -1);

			//			Put that new topic into the counts
			oneDocTopics[position] = newTopic;
                            
                        fp.update(newTopic, beta*(alpha[newTopic]+localTopicCounts[newTopic])/
                                (double)(tokensPerTopic[newTopic] + betaSum)*(tokensPerTopic[newTopic] + betaSum-1));
			localTopicCounts[newTopic]++;

			// If this is a new topic for this document,
			//  add the topic to the dense index.
			if (localTopicCounts[newTopic] == 1) {
				
				// First find the point where we 
				//  should insert the new topic by going to
				//  the end (which is the only reason we're keeping
				//  track of the number of non-zero
				//  topics) and working backwards

				denseIndex = nonZeroTopics;

				while (denseIndex > 0 &&
					   localTopicIndex[denseIndex - 1] > newTopic) {

					localTopicIndex[denseIndex] =
						localTopicIndex[denseIndex - 1];
					denseIndex--;
				}
				
				localTopicIndex[denseIndex] = newTopic;
				nonZeroTopics++;
			}

			tokensPerTopic[newTopic]++;

			//	update the coefficients for the non-zero topics
			cachedCoefficients[newTopic] =
				(alpha[newTopic] + localTopicCounts[newTopic]) /
				(tokensPerTopic[newTopic] + betaSum);
                                
                        fp.update(newTopic, -(beta*(alpha[newTopic]+localTopicCounts[newTopic]))/
                                (double)(tokensPerTopic[newTopic] + betaSum)*(tokensPerTopic[newTopic] + betaSum+1));
		}

		if (shouldSaveState) {
			// Update the document-topic count histogram,
			//  for dirichlet estimation
			docLengthCounts[ docLength ]++;

			for (denseIndex = 0; denseIndex < nonZeroTopics; denseIndex++) {
				int topic = localTopicIndex[denseIndex];
				
				topicDocCounts[topic][ localTopicCounts[topic] ]++;
			}
		}

		//	Clean up our mess: reset the coefficients to values with only
		//	smoothing. The next doc will update its own non-zero topics...

		for (denseIndex = 0; denseIndex < nonZeroTopics; denseIndex++) {
			int topic = localTopicIndex[denseIndex];

			cachedCoefficients[topic] =
				alpha[topic] / (tokensPerTopic[topic] + betaSum);
		}

	}
	// Fenwick tree implementation
    class FenwickPlus{
    
    public void construct(double[] dist){
    int l;
    if(dist.length>Integer.highestOneBit(dist.length))
     l= Integer.highestOneBit(dist.length)*2-1;
    else
     l= Integer.highestOneBit(dist.length)-1;
    int i=l+1,j=0;
    nonZeroIndex=i;
    //System.out.println("First index which is non zero "+nonZeroIndex);
    while(i<fenwick.length && j<dist.length)
    {
        fenwick[i]=dist[j];
        i++;
        j++;
    }
    //System.out.println("Last index which is non zero "+(i-1));
    for(i=l;i>0;--i)
        fenwick[i]=fenwick[2*i]+fenwick[2*i+1];
    }
    public void update(int it,double delta)
    {
        int i=nonZeroIndex+it;
        if(fenwick[i]!=0)
        {
            while(i>0)
            {    
                fenwick[i]+=delta;
                i=parent(i);
            }
        }
        else
            System.out.println("Error");
    }
    // Sampling from fenwick tree
    public int sample(double u)
    {
        //System.out.println(Arrays.toString(fenwick));
        int i=1;
        while(i<nonZeroIndex)
        {
            if(u>=fenwick[2*i])
            {
            	if(fenwick[2*i + 1]>0.0){
                    u-=fenwick[2*i];
                    i=2*i+1; 
                }
                else
                    i=2*i;
            }
            else
                i=2*i;    
        }   
        return i;
    }
    /*
    void printFenwick(double[] fenwick)
    {
        int n=fenwick.length;
        for(int i=1;i<n;++i)
        {
            System.out.print(fenwick[i]+" ");
        }
    }*/
    // Initialise the fenwick tree
    public FenwickPlus(int n)
    {
       //System.out.println(n);
       fenwick= new double[n*n]; 
    }
    int parent(int child)
    {
        if(child%2==1)
            return((child-1)/2);
        return child/2;
    }
    }

}
