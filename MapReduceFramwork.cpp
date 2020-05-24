#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <memory>
#include <queue>
#include "Barrier.h"

typedef struct {
    int id;
    pthread_t *thread;
    pthread_mutex_t *shuffle_mutex;
    std::vector<IntermediatePair> toShuffle;
    void *jc;
} ThreadContext;


class JobContext {
public:
    JobContext(const MapReduceClient &_client, InputVec _in_vec, OutputVec _out_vec, int multiThreadLevel):stage(UNDEFINED_STAGE),
    client(_client), in_vec(_in_vec), intermedita_vec(nullptr), k2Vec(nullptr), out_vec(_out_vec),
    threads(new std::vector<ThreadContext>[multiThreadLevel]), barrier(Barrier(multiThreadLevel))
    {
        atomic_init(&mapCounter, 0);
        atomic_init(&reduceCounter, 0);
        pthread_mutex_init(&map_mutex, nullptr);
        pthread_mutex_init(&reduce_mutex, nullptr);
    };

    ~JobContext(){delete[] threads;};

    stage_t stage;
    const MapReduceClient &client;
    const InputVec &in_vec;
    IntermediateMap *intermedita_vec;
    std::vector<K2 *> *k2Vec;
    OutputVec &out_vec;
    std::atomic<int> mapCounter;
    std::atomic<int> reduceCounter;
    std::vector<ThreadContext> *threads;
    pthread_mutex_t map_mutex;
    pthread_mutex_t reduce_mutex;
    Barrier barrier;

};


//typedef struct {
//    stage_t stage;
//    const MapReduceClient &client;
//    const InputVec &in_vec;
//    IntermediateMap *intermedita_vec;
//    std::vector<K2> *k2Vec;
//    OutputVec &out_vec;
//    std::atomic<int> mapCounter;
//    std::atomic<int> reduceCounter;
//    std::vector<ThreadContext> *threads;
//    pthread_mutex_t map_mutex;
//    pthread_mutex_t reduce_mutex;
//    Barrier barrier;
//
//} JobContext;

void reduceThread(void *args) {
    auto *tc = (ThreadContext *) args;
    auto *jc = (JobContext *) tc->jc;
    int tempReduceCounter;
    while (jc->reduceCounter < jc->k2Vec->size()) {
        pthread_mutex_lock(&jc->reduce_mutex);
        tempReduceCounter = jc->reduceCounter++;
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
        if (jc->mapCounter == jc->in_vec.size()) {
            pthread_mutex_unlock(&jc->map_mutex);
            break;
        }
        tempMapCounter = jc->mapCounter++;
        pthread_mutex_unlock(&jc->map_mutex);
        jc->client.map(jc->in_vec[tempMapCounter].first, jc->in_vec[tempMapCounter].second, &tc);
    }
    jc->barrier.barrier();
    //start reduce
    reduceThread(args);
    return nullptr;
}

void *shuffleThread(void *args) {
    auto *jc = (JobContext *) args;
    auto *toShuffle = new std::vector<IntermediatePair>;
    int pushedCounter = 0;
    while (pushedCounter < jc->in_vec.size()) {
        for (int i = 0; i < jc->threads->size(); i++) {
            if (!jc->threads->at(i).toShuffle.empty()) {

                pthread_mutex_lock(jc->threads->at(i).shuffle_mutex);
                toShuffle->swap(jc->threads->at(i).toShuffle);
                pthread_mutex_unlock(jc->threads->at(i).shuffle_mutex);
                for (auto &element : *toShuffle) {
                    jc->intermedita_vec->at(element.first).push_back(element.second);
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
//            .stage =   UNDEFINED_STAGE,
//            .client =   client,
//            .in_vec =   inputVec,
//            .intermedita_vec =  nullptr,
//            .k2Vec = nullptr,
//            .out_vec = outputVec,
//            .threads = new std::vector<ThreadContext>[multiThreadLevel],
//            .barrier = Barrier(multiThreadLevel),
//
//    ));

    atomic_init(&jc->mapCounter, 0);
    atomic_init(&jc->reduceCounter, 0);
    pthread_mutex_init(&jc->map_mutex, nullptr);
    pthread_mutex_init(&jc->reduce_mutex, nullptr);

    //create threads and their respective mutexes
    for (int i = 1; i < multiThreadLevel; i++)
    {
        jc->threads->at(i).id = i;
        jc->threads->at(i).jc = &jc;
        pthread_mutex_init(jc->threads->at(i).shuffle_mutex, nullptr);
        pthread_create(jc->threads->at(i).thread, nullptr, mapThread, &jc->threads->at(i));
    }
    pthread_create(jc->threads->at(0).thread, nullptr, mapThread, &jc);
    jc->threads->at(0).shuffle_mutex = nullptr; //just for safety

    //barrier
    //reduce
    //return job handle

}

void emit2(K2 *key, V2 *value, void *context) {
    auto *tc = (ThreadContext *) context;
    IntermediatePair tempPair(key, value);
    pthread_mutex_lock(tc->shuffle_mutex);
    tc->toShuffle.push_back(tempPair);
    pthread_mutex_unlock(tc->shuffle_mutex);
}

void emit3(K3 *key, V3 *value, void *context) {
    auto *jc = (JobContext *) context;

    OutputPair tempPair(key, value);
    pthread_mutex_lock(&jc->reduce_mutex);
    jc->out_vec.emplace_back(key, value);
    pthread_mutex_unlock(&jc->reduce_mutex);
}


