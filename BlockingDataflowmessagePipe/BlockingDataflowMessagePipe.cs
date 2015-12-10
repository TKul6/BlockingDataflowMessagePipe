using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BlockingDataflowmessagePipe
{
    public class BlockingDataflowMessagePipe<TMessage>
    {

        private BatchBlock<TMessage> mBatchBlock;

        private Action<TMessage> mUserAction;
        private int mCount;

        public int Count
        {
            get { return mCount; }
            private set { mCount = value; }
        }

        private const int INTERNAL_BUFFER = 2;

        public BlockingDataflowMessagePipe(Action<TMessage> userAction, int bulkSize, int maxParallelThreads, int queueLimit)
        {
            mUserAction = userAction;

            mBatchBlock = new BatchBlock<TMessage>(bulkSize, new GroupingDataflowBlockOptions() { Greedy = true, BoundedCapacity = queueLimit});


            //TODO: not sure if the buffered message should be substract from queue size limit
            //Please notice : the block must buffer at least 1 message so we decrement that from the BatchBlock bounded capacity
            var transformToInt = new TransformBlock<TMessage[], int>(messages => messages.Length, new ExecutionDataflowBlockOptions() { BoundedCapacity = 1 });


            var userActionBlock = new ActionBlock<TMessage[]>(messages =>
       {
           foreach (var message in messages)
           {
               mUserAction(message);
           }

       }, 
//Please notice : the block must buffer at least 1 message so we decrement that from the BatchBlock bounded capacity
           new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = maxParallelThreads, BoundedCapacity = 1 });

            mCount = 0;


            //Please notice : the block must buffer at least 1 message so we decrement that from the BatchBlock bounded capacity
            //this should be BroadcastBlock, but we don't want to lost messages ///
            var broeadcaster = new ActionBlock<TMessage[]>(async (messages) =>
            {
                await Task.WhenAll(transformToInt.SendAsync(messages), userActionBlock.SendAsync(messages));

            }, new ExecutionDataflowBlockOptions() { BoundedCapacity = 1 });


            //Please notice : the block must buffer at least 1 message  (but the message is an int, and the action is very small so we can assume it wont affacr the queue size)
            var decrementor =
                new ActionBlock<int>(
                    messagesToExecuteCount => Interlocked.Exchange(ref mCount, mCount - messagesToExecuteCount), new ExecutionDataflowBlockOptions() { BoundedCapacity = 1 });



            mBatchBlock.LinkTo(broeadcaster, new DataflowLinkOptions());


            transformToInt.LinkTo(decrementor);


        }

        public  bool AddMessage(TMessage message)
        {
            var result = mBatchBlock.SendAsync(message).Result;
            if (result)
            {
                Interlocked.Increment(ref mCount);
            }

            return result;
        }
    }
}
