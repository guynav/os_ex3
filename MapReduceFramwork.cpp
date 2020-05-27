#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <memory>
#include <queue>
#include "Barrier.h"

typedef struct {
    int id;
    pthread_t *thread;
    pthread_mutex_t shuffle_mutex;
    std::vector<IntermediatePair> * toShuffle;
    void *jc;
} ThreadContext;

void *mapThread(void *args);
void *shuffleThread(void *args);

class JobContext
{
public:
    JobContext(const MapReduceClient &_client, const InputVec& _in_vec, OutputVec& _out_vec, int
    multiThreadLevel)
    : stage(UNDEFINED_STAGE), client(_client), in_vec(_in_vec), intermedita_vec(nullptr), k2Vec
    (nullptr), out_vec(_out_vec),
    barrier(Barrier(multiThreadLevel)), threadAmount(multiThreadLevel)
    {
        threads = new ThreadContext[multiThreadLevel];
        intermedita_vec = new IntermediateMap();
        atomic_init(&mapCounter, 0);
        atomic_init(&reduceCounter, 0);
        pthread_mutex_init(&map_mutex, nullptr);
        pthread_mutex_init(&reduce_mutex, nullptr);


        for(int i = 1; i< multiThreadLevel; i++)
        {
            threads[i].id = i;
            threads[i].toShuffle = new std::vector<IntermediatePair>();
            threads[i].thread = new pthread_t;
            threads[i].jc = this;
            pthread_mutex_init(&threads[i].shuffle_mutex, nullptr);
            //help me, I'm dying.
        }
        threads[0].id = 0;
        threads[0].toShuffle = new std::vector<IntermediatePair>();
        threads[0].thread = new pthread_t;
        threads[0].jc = this;
        pthread_mutex_init(&threads[0].shuffle_mutex, nullptr);
        //pthread_create(threads[0].thread, nullptr, shuffleThread, &threads[0]);
    };
    ~JobContext()
    {
        for (int i = 0; i<threadAmount; i++)
        {
            delete threads[i].toShuffle;
            delete &threads[i].shuffle_mutex;
            delete threads[i].thread;
            delete threads[i].toShuffle;
        }
        delete intermedita_vec;
        delete[] threads;
    };

    stage_t stage;
    const MapReduceClient &client;
    const InputVec &in_vec;
    IntermediateMap *intermedita_vec;
    std::vector<K2 *> *k2Vec;
    OutputVec &out_vec;
    std::atomic<int> mapCounter;
    std::atomic<int> reduceCounter;
    ThreadContext * threads;
    pthread_mutex_t map_mutex;
    pthread_mutex_t reduce_mutex;
    Barrier barrier;
    int threadAmount;
};


void reduceThread(void *args) {
    auto *tc = (ThreadContext *) args;
    auto *jc = (JobContext *) tc->jc;
    int tempReduceCounter;
    while ((int) jc->reduceCounter < jc->k2Vec->size()) {
//        pthread_mutex_lock(&(jc->reduce_mutex));
        pthread_mutex_lock(&(jc->map_mutex));
        tempReduceCounter = jc->reduceCounter++;
//        pthread_mutex_unlock(&jc->map_mutex);
        pthread_mutex_unlock(&jc->map_mutex);
        K2  * currKey = jc->k2Vec->at(tempReduceCounter);
        jc->client.reduce(currKey, jc->intermedita_vec->find(currKey)->second, (void *) jc);
    }
}

void *mapThread(void *args) {
    auto *tc = (ThreadContext *) args;
    auto *jc = (JobContext *) tc->jc;
    jc->stage = MAP_STAGE;
    int tempMapCounter;
    while (jc->mapCounter < jc->in_vec.size()) {
        pthread_mutex_lock(&(jc->map_mutex));
        if (jc->mapCounter == jc->in_vec.size())
        {
            pthread_mutex_unlock(&jc->map_mutex);
            break;
        }
        tempMapCounter = jc->mapCounter;
        jc->mapCounter++;;
        pthread_mutex_unlock(&jc->map_mutex);
        jc->client.map(jc->in_vec[tempMapCounter].first, jc->in_vec[tempMapCounter].second, args);
    }
    jc->barrier.barrier();
    //start reduce
    reduceThread(args);
    return nullptr;
}

void *shuffleThread(void *args)
{
    auto tc = (ThreadContext*) args;
    auto jc = (JobContext *) tc->jc;
    auto *toShuffle = new std::vector<IntermediatePair>;
    int pushedCounter = 0;
    while (pushedCounter < jc->in_vec.size()) {
        for (int i = 1; i < jc->threadAmount; i++) {
            if (!jc->threads[i].toShuffle->empty())
            {

                pthread_mutex_lock(&jc->threads[i].shuffle_mutex);
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
                pushedCounter += toShuffle->size();
                toShuffle->clear();
            }
        }
    }
    delete toShuffle; //might delete automatically
    jc->k2Vec = new std::vector<K2 *>();
    for (auto element = jc->intermedita_vec->begin(); element != jc->intermedita_vec->end(); ++element) {
        jc->k2Vec->push_back(element->first);
    }
    pthread_mutex_unlock(&jc->reduce_mutex);
    jc->barrier.barrier();
    jc->stage = REDUCE_STAGE;
    reduceThread(args);
    return nullptr;
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {
    //create job context

    std::shared_ptr<JobContext> jc(new JobContext(client,inputVec, outputVec, multiThreadLevel));
    //create threads and their respective mutexes
    for(int i = 1; i< multiThreadLevel; i++)
    {
        auto stam = jc->threads[i];
        pthread_create(jc->threads[i].thread, nullptr, mapThread, &jc->threads[i]);

        //help me, I'm dying.
    }
    pthread_create(jc->threads[0].thread, nullptr, shuffleThread, &jc->threads[0]);


    //barrier
    //reduce
    //return job handle
    return (JobHandle) &(*jc);
}

void emit2(K2 *key, V2 *value, void *context) {
    auto tc = (ThreadContext *) context;
    IntermediatePair tempPair(key, value);
    pthread_mutex_lock(&tc->shuffle_mutex);
    tc->toShuffle->push_back(tempPair);
    pthread_mutex_unlock(&tc->shuffle_mutex);
}

void emit3(K3 *key, V3 *value, void *context) {
    auto *jc = (JobContext *) context;

    OutputPair tempPair(key, value);
    pthread_mutex_lock(&jc->reduce_mutex);
    jc->out_vec.emplace_back(key, value);
    pthread_mutex_unlock(&jc->reduce_mutex);
}

void getJobState(JobHandle job, JobState * state)
{
    JobContext * jc = (JobContext*) job;
    state->stage = jc->stage;
    switch(jc->stage){
        case UNDEFINED_STAGE:
            state->percentage = 0;
            break;
        case MAP_STAGE:
            state->percentage = 100 * ((float) jc->mapCounter / (float) jc->in_vec.size());
            break;
        case REDUCE_STAGE:
            state->percentage  = 100 * ((float )  jc->reduceCounter /(float) jc->intermedita_vec->size());
    }
}

void waitForJob(JobHandle job)
{
    auto jc = (JobContext*) job;
    for(int i = 0; i < jc->threadAmount; i++){
        pthread_join(*jc->threads[i].thread, nullptr);
    }
}

void closeJobHandle(JobHandle job)
{
    waitForJob(job);
    auto jc = (JobContext*) job;
    delete  jc;
}


