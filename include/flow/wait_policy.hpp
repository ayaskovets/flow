#pragma once

#include <atomic>
#include <stop_token>

namespace flow::concepts {

template <typename T>
concept wait_policy =
    requires(T policy, std::atomic<std::size_t> * seqnum,
             std::size_t expected_seqnum, std::stop_token const & stop_token) {
      { policy.wait_not_full(seqnum, expected_seqnum, stop_token) };
      { policy.wait_not_empty(seqnum, expected_seqnum, stop_token) };
      { policy.notify_not_full(seqnum) };
      { policy.notify_not_empty(seqnum) };
      { policy.yield_contention() };
    };

}  // namespace flow::concepts

// struct ThreadFutexStrategy {
//   static void wait_not_full(std::atomic<size_t> * seq, size_t expected,
//                             std::stop_token const & st) {
//     if (st.stop_requested()) return;
//     // Засыпаем в ядре ОС до тех пор, пока *seq == expected
//     seq->wait(expected, std::memory_order::relaxed);
//   }

//   static void wait_not_empty(std::atomic<size_t> * seq, size_t expected,
//                              std::stop_token const & st) {
//     if (st.stop_requested()) return;
//     seq->wait(expected, std::memory_order::relaxed);
//   }

//   static void notify_not_full(std::atomic<size_t> * seq) {
//     seq->notify_all();  // Будим писателей, ждущих этот слот
//   }

//   static void notify_not_empty(std::atomic<size_t> * seq) {
//     seq->notify_all();  // Будим читателей, ждущих этот слот
//   }

//   static void yield_contention() {
//     // Легкая пауза процессора, чтобы не забивать шину памяти при CAS-шторме
//     __builtin_ia32_pause();  // или std::this_thread::yield()
//   }
// };

// struct CoroutineEngineStrategy {
//   static void wait_not_full(std::atomic<size_t> * seq, size_t expected,
//                             std::stop_token const & st) {
//     // Указываем рантайму: "Приостанови текущую исполняемую задачу
//     // (корутину/файбер) и верни её в пулл потоков только тогда, когда
//     условие
//     // станет истинным"
//     MyTaskEngine::suspend_current_until([seq, expected, &st]() {
//       return st.stop_requested() ||
//              (seq->load(std::memory_order_relaxed) != expected);
//     });
//   }

//   static void wait_not_empty(std::atomic<size_t> * seq, size_t expected,
//                              std::stop_token const & st) {
//     MyTaskEngine::suspend_current_until([seq, expected, &st]() {
//       return st.stop_requested() ||
//              (seq->load(std::memory_order_relaxed) != expected);
//     });
//   }

//   static void notify_not_full(std::atomic<size_t> * /*seq*/) {
//     // В зависимости от архитектуры движка:
//     // Либо движок сам опрашивает условия (Polling), тогда здесь ничего
//     делать
//     // не нужно. Либо мы пингуем Event Loop движка, чтобы он проверил
//     // приостановленные задачи.
//     MyTaskEngine::wake_up_waiters();
//   }

//   static void notify_not_empty(std::atomic<size_t> * /*seq*/) {
//     MyTaskEngine::wake_up_waiters();
//   }

//   static void yield_contention() {
//     // Отдаем управление диспетчеру задач, чтобы не блокировать worker-поток
//     MyTaskEngine::yield_current_task();
//   }
// };
