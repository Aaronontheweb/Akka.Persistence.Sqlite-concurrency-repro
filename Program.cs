using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Pattern;
using Akka.Persistence;
using Akka.Util;

namespace Akka.Persistence.StressTest
{
    class Program
    {
        private static Config config = ConfigurationFactory.ParseString(@"
            akka {
                suppress-json-serializer-warning = true
                persistence.journal {
                    plugin = ""akka.persistence.journal.sqlite""
                    sqlite {
                        class = ""Akka.Persistence.Sqlite.Journal.BatchingSqliteJournal, Akka.Persistence.Sqlite""
                        plugin-dispatcher = ""akka.actor.default-dispatcher""
                        table-name = event_journal
                        metadata-table-name = journal_metadata
                        auto-initialize = on
                        connection-string = ""Datasource=memdb-journal.db;""
                    }
                }

                persistence.snapshot-store{
                    plugin = ""akka.persistence.snapshot-store.sqlite""
                    sqlite{
                        class = ""Akka.Persistence.Sqlite.Snapshot.SqliteSnapshotStore, Akka.Persistence.Sqlite""
                        plugin-dispatcher = ""akka.actor.default-dispatcher""
                        auto-initialize = on
                        connection-string = ""Datasource=memdb-journal.db;""
                    }
                }
            }");

        public const int ActorCount = 100;
        public const int MessagesPerActor = 1000;


        static void Main(string[] args)
        {
            using (var system = ActorSystem.Create("persistent-benchmark", config.WithFallback(ConfigurationFactory.Default())))
            {
                Console.WriteLine("Performance benchmark starting...");

                var actors = new IActorRef[ActorCount];
                for (int i = 0; i < ActorCount; i++)
                {
                    var pid = "a-" + i;
                    actors[i] = system.ActorOf(ReceiverActor.Props(pid, ThreadLocalRandom.Current.Next(5,20)), pid);
                }

                Task.WaitAll(actors.Select(a => a.Ask<Done>(Init.Instance)).Cast<Task>().ToArray());

                Console.WriteLine("All actors have been initialized...");

                var stopwatch = new Stopwatch();
                stopwatch.Start();

                for (int i = 0; i < MessagesPerActor; i++)
                    for (int j = 0; j < ActorCount; j++)
                    {
                        actors[j].Tell(new Message("hi"));
                    }

                var finished = new Task[ActorCount];
                for (int i = 0; i < ActorCount; i++)
                {
                    finished[i] = actors[i].Ask<Finished>(Finish.Instance);
                }

                Task.WaitAll(finished);

                var elapsed = stopwatch.ElapsedMilliseconds;

                Console.WriteLine($"{ActorCount} actors stored {MessagesPerActor} events each in {elapsed / 1000.0} sec. Average: {ActorCount * MessagesPerActor * 1000.0 / elapsed} events/sec");

                foreach (Task<Finished> task in finished)
                {
                    if (!task.IsCompleted || task.Result.State != MessagesPerActor)
                        throw new IllegalStateException("Actor's state was invalid");
                }
            }

            Console.ReadLine();
        }
    }

    public class ReceiverActor : ReceivePersistentActor, IWithUnboundedStash
    {
        private const int DefaultSnapshotSize = 100;
        private readonly int _snapshotSize = 0;
        private int _messagesSinceSnapshot = 0;
        private readonly ILoggingAdapter _log = Context.GetLogger();

        private List<Message> _messages = new List<Message>();

        private long _count;

        public static Props Props(string actorId, int? snapshotSize = null)
        {
            return Akka.Actor.Props.Create(
                () => new ReceiverActor(actorId, snapshotSize ?? DefaultSnapshotSize));
        }

        public ReceiverActor(string actorId, int snapshotSize)
        {
            _snapshotSize = snapshotSize;

            PersistenceId = nameof(ReceiverActor) + "." + actorId;

            // Recover<Message>();

            Recover<SnapshotOffer>(snapshot => RecoverFromSnapshot(snapshot));

            Processing();
        }

        private void Processing()
        {
            Command<Message>(message => HandleMessage(message));

            Command<Init>(_ => Sender.Tell(Done.Instance));

            Command<Finish>(_ => Sender.Tell(new Finished(_count)));
        }

        private void RecoverFromSnapshot(SnapshotOffer snapshot)
        {
            // recover state from snapshot
            if (snapshot.Snapshot is List<Message> message)
            {
                _messages = message;
            }
        }

        private void SaveSnapshotOnThreshold<T>(T message, Action<T> messageHandler)
        {
            messageHandler(message);
            if (++_messagesSinceSnapshot % _snapshotSize == 0)
            {
                SaveSnapshot(GetCurrentMessages());
                _messages.Clear();
                Become(SnapShotting);
            }
        }

        private void SnapShotting()
        {
            Command<SaveSnapshotSuccess>(success =>
            {
                _log.Info("Deleting messages up to SeqNo [{0}]", success.Metadata.SequenceNr);
                DeleteMessages(success.Metadata.SequenceNr);
                Become(Processing);
                Stash.UnstashAll();
            });

            Command<SaveSnapshotFailure>(failure =>
            {
                _log.Error(failure.Cause, "failed to persist snapshot");
                Become(Processing);
                Stash.UnstashAll();
            });

            CommandAny(_ => Stash.Stash());
        }

        private List<Message> GetCurrentMessages()
        {
            // retrieves the current messages from actor state
            return _messages;
        }

        private void HandleMessage(Message message)
        {
            
            Persist(message,
                e => SaveSnapshotOnThreshold(e, entry => ProcessMessage(message)));
        }

        private void ProcessMessage(Message message)
        {
            _count++;
            // store the message in actor state
            _messages.Add(message);
        }

        public override string PersistenceId { get; }
    }

    public class Message
    {
        public Message(string str)
        {
            Str = str;
        }

        public string Str { get; }
    }

    public sealed class Init
    {
        public static readonly Init Instance = new Init();
        private Init() { }
    }

    public sealed class Finish
    {
        public static readonly Finish Instance = new Finish();
        private Finish() { }
    }
    public sealed class Done
    {
        public static readonly Done Instance = new Done();
        private Done() { }
    }
    public sealed class Finished
    {
        public readonly long State;

        public Finished(long state)
        {
            State = state;
        }
    }
}
