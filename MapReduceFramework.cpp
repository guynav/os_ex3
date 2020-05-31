#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <queue>
#include <iostream>
#include "Barrier.h"

static const char * const ERROR_MSG = "system error:";
/**
 * THis struct represents the context in which a thread works, including its id, mutexes and job context. Also stores
 * key value pairs which have already been mapped.
 */
typedef struct
{
    int id;
    pthread_t * thread;
    pthread_mutex_t shuffle_mutex;
    std::vector<IntermediatePair> * toShuffle;
    void * jc;
} ThreadContext;

void * mapThread(void * args);

void * shuffleThread(void * args);

/**
 * THIs class represents the context of a job done, including threads
 */

class JobContext
{
public:
    /**
     * COnstructor for this class
     * @param _client the client for this job
     * @param _in_vec the input vector
     * @param _out_vec the output vector
     * @param multiThreadLevel the amount fo threads to be used in this job
     */
    JobContext(const MapReduceClient &_client, const InputVec &_in_vec, OutputVec &_out_vec, int
    multiThreadLevel)
            : stage(UNDEFINED_STAGE), client(_client), in_vec(_in_vec), intermedita_vec(nullptr), k2Vec
            (nullptr), out_vec(_out_vec),
              barrier(Barrier(multiThreadLevel)), threadAmount(multiThreadLevel), doneMap(false)
    {
        try
        {
            threads = new ThreadContext[multiThreadLevel];
            intermedita_vec = new IntermediateMap();
        }
        catch (std::bad_alloc &ba)
        {
            std::cerr << ERROR_MSG << "bad_alloc caught: " << ba.what() << '\n';
            exit(1);
        }

        atomic_init(&mapCounter, 0);
        atomic_init(&reduceCounter, 0);
        atomic_init(&mapFinishCounter, 0);
        pthread_mutex_init(&map_mutex, nullptr);
        pthread_mutex_init(&reduce_mutex, nullptr);


        for (int i = 0; i < multiThreadLevel; i++)
        {
            threads[i].id = i;
            try
            {
                threads[i].toShuffle = new std::vector<IntermediatePair>();
                threads[i].thread = new pthread_t;
            } catch (std::bad_alloc &ba)
            {
                std::cerr << ERROR_MSG << "bad_alloc caught: " << ba.what() << '\n';
                exit(1);
            }
            threads[i].jc = this;
            pthread_mutex_init(&threads[i].shuffle_mutex, nullptr);
            //help me, I'm dying.
        }
    };

    /**
     * Destructor for this class
     */
    ~JobContext()
    {
        for (int i = 0; i < threadAmount; i++)
        {
            pthread_mutex_lock(&threads[i].shuffle_mutex);
            delete threads[i].toShuffle;
            pthread_mutex_unlock(&threads[i].shuffle_mutex);
            pthread_mutex_destroy(&threads[i].shuffle_mutex);
            delete threads[i].thread;
//            delete threads[i].toShuffle;
        }
        delete[] threads;
        delete intermedita_vec;
        delete k2Vec;
        pthread_mutex_destroy(&reduce_mutex);
        pthread_mutex_destroy(&map_mutex);
//        delete &mapCounter;
//        delete &reduceCounter;
    };

    stage_t stage;
    const MapReduceClient &client;
    const InputVec &in_vec;
    IntermediateMap * intermedita_vec;
    std::vector<K2 *> * k2Vec;
    OutputVec &out_vec;
    std::atomic<int> mapCounter;
    std::atomic<int> reduceCounter;
    ThreadContext * threads;
    pthread_mutex_t map_mutex;
    pthread_mutex_t reduce_mutex;
    Barrier barrier;
    int threadAmount;
    std::atomic<int> mapFinishCounter;
    bool doneMap;
};

/**
 * This function performs reduce operation on intermediate vector input
 * @param args the context of this reduce
 */

void reduceThread(void * args)
{
    auto * tc = (ThreadContext *) args;
    auto * jc = (JobContext *) tc->jc;
    int tempReduceCounter;
    while ((int) jc->reduceCounter < jc->k2Vec->size())
    {
        pthread_mutex_lock(&(jc->reduce_mutex));
//        pthread_mutex_lock(&(jc->map_mutex));
        tempReduceCounter = jc->reduceCounter++;

        pthread_mutex_unlock(&jc->reduce_mutex);

        if (tempReduceCounter >= jc->k2Vec->size())
        {
            break;
        }
        K2 * currKey = jc->k2Vec->at(tempReduceCounter);

        jc->client.reduce(currKey, jc->intermedita_vec->find(currKey)->second, (void *) jc);
    }
}

/**
 * THis funciton performs the map operation on input vector
 * @param args the thread context of this map operation
 * @return nullptr
 */
void * mapThread(void * args)
{
    auto * tc = (ThreadContext *) args;
    auto * jc = (JobContext *) tc->jc;
    int tempMapCounter;
    while (jc->mapCounter < jc->in_vec.size())
    {
        pthread_mutex_lock(&(jc->map_mutex));
        if (jc->mapCounter == jc->in_vec.size())
        {
            pthread_mutex_unlock(&jc->map_mutex);
            break; //todo ask whether all thtreads should be initiated before starting to run
        }
        tempMapCounter = jc->mapCounter++; // todo added the '++' after map counter
        pthread_mutex_unlock(&jc->map_mutex);
        jc->client.map(jc->in_vec[tempMapCounter].first, jc->in_vec[tempMapCounter].second, args);
    }
    int tempc;
    pthread_mutex_lock(&jc->reduce_mutex);
    tempc = jc->mapFinishCounter++;
    pthread_mutex_unlock(&jc->reduce_mutex);
    if(tempc == jc->threadAmount - 2){
        jc->doneMap = true;
    }
    jc->barrier.barrier();
    //start reduce
    reduceThread(args);
    return nullptr;
}




/**
 * THIs function performs the shuffle operation on threads
 * @param args the context of the thread performing this operation
 * @return nullptr
 */
void * shuffleThread(void * args)
{
    auto tc = (ThreadContext *) args;
    auto jc = (JobContext *) tc->jc;
    auto * toShuffle = tc->toShuffle;
    bool notLastround = true;
    int pushedCounter = 0;
    jc->stage = MAP_STAGE; //todo verify this is the correct stage and not shuffle
    while (!jc->doneMap or notLastround)
    {
        if(jc->doneMap){
            notLastround = false;
        }

        for (int i = 1; i < jc->threadAmount; i++)
        {
            pthread_mutex_lock(&jc->threads[i].shuffle_mutex);
            if (!jc->threads[i].toShuffle->empty())
            {
                toShuffle->swap(*(jc->threads[i].toShuffle));
                pthread_mutex_unlock(&jc->threads[i].shuffle_mutex);
                for (auto &element : *toShuffle)
                {
                    if (jc->intermedita_vec->count(element.first))
                    {
                        jc->intermedita_vec->at(element.first).push_back(element.second);
                    }
                    else //need to create key vector
                    {
                        jc->intermedita_vec->emplace(element.first, std::vector<V2 *>());
                        jc->intermedita_vec->at(element.first).push_back(element.second);
                    }
                }
                toShuffle->clear();
            }
            else
            {
                pthread_mutex_unlock(&jc->threads[i].shuffle_mutex);
            }
        }
    }
    try
    {
        jc->k2Vec = new std::vector<K2 *>();
    } catch (std::bad_alloc &ba)
    {
        std::cerr << ERROR_MSG << "bad_alloc caught: " << ba.what() << '\n';
        exit(1);
    }

    for (auto element = jc->intermedita_vec->begin(); element != jc->intermedita_vec->end(); ++element)
    {
        jc->k2Vec->push_back(element->first);
    }
    pthread_mutex_unlock(&jc->reduce_mutex);
    jc->barrier.barrier();
    jc->stage = REDUCE_STAGE;
    reduceThread(args);
    return nullptr;
}

/**
 * THIs funciton starts running the MapREduce algorithm (with several threads)
 * @param client THe task the framework should run
 * @param inputVec vector of type std::vector<std::pair<K1*, V1*>>
 * @param outputVec vector of type std::vector<std::pair<K3*, V3*>> to which the output elements will be added before
 * returning
 * @param multiThreadLevel the number of worker threads to be used for running the algorithm
 * @return JobHandle
 */
JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{
    //create job context
    JobContext * jc;
    try
    {
        jc = new JobContext(client, inputVec, outputVec, multiThreadLevel);
    }
    catch (std::bad_alloc &ba)
    {
        std::cerr << ERROR_MSG << "bad_alloc caught: " << ba.what() << '\n';
        exit(1);
    }
//    std::shared_ptr<JobContext> jc = std::make_shared<JobContext>(  (JobContext(client,inputVec, outputVec, multiThreadLevel)));
    //create threads and their respective mutexes
    for (int i = 1; i < multiThreadLevel; i++)
    {
//        auto stam = jc->threads[i];
        if (pthread_create(jc->threads[i].thread, nullptr, mapThread, &jc->threads[i]))
        {
            std::cerr << ERROR_MSG << " Could not create thread" << '\n';
            exit(1);
        }
        //help me, I'm dying.
    }
    if (pthread_create(jc->threads[0].thread, nullptr, shuffleThread, &jc->threads[0]))
    {
        std::cerr << ERROR_MSG << " Could not create thread" << '\n';
        exit(1);
    }


    //barrier
    //reduce
    //return job handle
    return (JobHandle) jc;
}

/**
 * This function produces (K2*, V2*)pair
 * @param key the key of the pair
 * @param value the value of the pair
 * @param context object to produce context from
 */
void emit2(K2 * key, V2 * value, void * context)
{
    auto tc = (ThreadContext *) context;
    IntermediatePair tempPair(key, value);
    pthread_mutex_lock(&tc->shuffle_mutex);
    tc->toShuffle->push_back(tempPair);
    pthread_mutex_unlock(&tc->shuffle_mutex);
}

/**
 * This function produces (K3*, V3*)pair
 * @param key the key of the pair
 * @param value the value of the pair
 * @param context object to produce context from
 */
void emit3(K3 * key, V3 * value, void * context)
{
    auto * jc = (JobContext *) context;

    OutputPair tempPair(key, value);
    pthread_mutex_lock(&jc->reduce_mutex);
    jc->out_vec.emplace_back(key, value);
    pthread_mutex_unlock(&jc->reduce_mutex);
}

/**
 * This function gets a job handle and updates the state opf the job into the given JObState struct
 */
void getJobState(JobHandle job, JobState * state)
{
    JobContext * jc = (JobContext *) job;
    bool cond;
    float curPercent;
    state->stage = jc->stage;
    switch (state->stage)
    {
        case UNDEFINED_STAGE:
            state->percentage = 0;
            break;
        case MAP_STAGE:
            curPercent = 100 * ((float) jc->mapCounter / (float) jc->in_vec.size());
            state->percentage = curPercent < 100 ? curPercent : 100;
            break;
        case REDUCE_STAGE:
            curPercent = 100 * ((float) jc->reduceCounter / (float) jc->intermedita_vec->size());
            state->percentage = curPercent < 100 ? curPercent : 100;
    }
}

/**
 * This function gets a job handle returned by startMapREduceFramework and waits until it is finished
 * @param job
 */
void waitForJob(JobHandle job)
{
    auto jc = (JobContext *) job;
    for (int i = 0; i < jc->threadAmount; i++)
    {
        pthread_join(*jc->threads[i].thread, nullptr);
    }
}

/**
 * Releases all resources of a job. Prevents resource release before the job is finished. Job handle is invalid after
 * this call
 * @param job the current job
 */
void closeJobHandle(JobHandle job)
{
    waitForJob(job);
    auto jc = (JobContext *) job;
    delete jc;
}


