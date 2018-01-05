
import java.util.Arrays;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author blackhat
 */
public class QuickSort
{
    int partition(int wordTypeTopicCounts[], int low, int high,int wordTypeTopicIndex[])
    {
        int pivot = wordTypeTopicCounts[wordTypeTopicIndex[high]] ;
        int i = (low-1); // index of smaller element
        for (int j=low; j<high; j++)
        {
            // If current element is smaller than or
            // equal to pivot
            if ((wordTypeTopicCounts[wordTypeTopicIndex[j]]) >= pivot)
            {
                i++;

                // swap arr[i] and arr[j]
                int temp = wordTypeTopicIndex[i];
                wordTypeTopicIndex[i] = wordTypeTopicIndex[j];
                wordTypeTopicIndex[j] = temp;
            }
        }

        // swap arr[i+1] and arr[high] (or pivot)
        int temp = wordTypeTopicIndex[i+1];
        wordTypeTopicIndex[i+1] = wordTypeTopicIndex[high];
        wordTypeTopicIndex[high] = temp;

        return i+1;
    }


    void sort(int wordTypeTopicCounts[], int low, int high,int wordTypeTopicIndex[])
    {
        if (low < high)
        {
            int pi = partition(wordTypeTopicCounts, low, high,wordTypeTopicIndex);

            sort(wordTypeTopicCounts, low, pi-1,wordTypeTopicIndex);
            sort(wordTypeTopicCounts, pi+1, high,wordTypeTopicIndex);
        }
    }
    int binarySearch(int wordTypeTopicCounts[],int y,int wordTypeTopicIndex[],int key)
    {
        int l = 0, r = y - 1;
        
        while (l <= r)
        {
            int m = l + (r-l)/2;
            try
            {
            // Check if x is present at mid
            //System.out.println("wordTypeTopicCounts "+wordTypeTopicCounts[wordTypeTopicIndex[m]]+" wordTypeTopicIndex "+wordTypeTopicIndex[m]);
                if ((wordTypeTopicCounts[wordTypeTopicIndex[m]]) == key)
                    return m;

            // If x greater, ignore left half
                if ((wordTypeTopicCounts[wordTypeTopicIndex[m]])< key)
                    l = m + 1;

            // If x is smaller, ignore right half
                else
                    r = m - 1;
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
       
         }
        // if we reach here, then element was not present
        return -1;
    }
    public static void main(String[] args)
    {
        int[] arr={7,0,8,0,6,9,2,1,3,0,0,4};
        int[] ind=new int[arr.length];
        for(int i=0;i<arr.length;++i)
        {
            ind[i]=i;
        }
        QuickSort qs= new QuickSort();
        qs.sort(arr, 0, arr.length-1, ind);
        System.out.println(Arrays.toString(arr)+"\n"+Arrays.toString(ind));
        System.out.println();
        for(int i=0;i<arr.length;++i)
        {
            System.out.print(arr[ind[i]]+" ");
        }
        
    }
}
